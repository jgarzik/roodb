# RooDB Architecture

Distributed SQL database in Rust. TLS-only networking, Raft consensus, MVCC transactions, Volcano executor, LSM storage.

## Process, Thread, and Task Model

**Single process** - the `roodb` binary. No `fork()`, no child processes. Single failure domain.

**Thread Pools**:
```
┌─────────────────────────────────────┐
│  Tokio Runtime                      │
│                                     │
│  ┌─────────────────────────────┐    │
│  │ Worker Pool (~1 per core)   │    │
│  │ - all async tasks           │    │
│  │ - network I/O               │    │
│  │ - io_uring completions      │    │
│  │ - CPU work (parse/plan/exec)│    │
│  └─────────────────────────────┘    │
│                                     │
│  ┌─────────────────────────────┐    │
│  │ Blocking Pool (elastic)     │    │
│  │ - spawn_blocking() only     │    │
│  │ - POSIX file I/O (non-Linux)│    │
│  └─────────────────────────────┘    │
└─────────────────────────────────────┘
```

**Tasks vs Threads**: Tasks are lightweight async coroutines (green threads), not OS threads. Thousands of connections share a handful of OS threads. Each connection spawns one task via `tokio::spawn`.

**Yielding Behavior**:

| Phase | Yields? | Notes |
|-------|---------|-------|
| Network read/write | Yes | Async I/O |
| Storage read/write | Yes | Async I/O (io_uring or spawn_blocking) |
| Parse SQL | No | Synchronous CPU |
| Resolve/TypeCheck | No | Synchronous CPU |
| Plan (logical/physical) | No | Synchronous CPU |
| Filter/Project per row | No | CPU eval |
| Sort/Aggregate | No | In-memory, CPU-bound |

**Implication**: CPU-heavy queries (complex planning, large in-memory sorts) hold the OS thread without yielding. Under high concurrency with CPU-bound workloads, tasks queue up. I/O-bound workloads scale well.

**Shared State** (via `Arc<>`):
- `StorageEngine` - LSM storage backend
- `Catalog` - schema metadata (wrapped in `RwLock`)
- `TransactionManager` - MVCC coordination
- `RaftNode` - consensus and replication

**Per-Connection State**:
- `Session` - user, database, transaction state, isolation level
- `PacketReader`/`PacketWriter` - protocol framing
- Connection ID (atomic counter)

## I/O Patterns

**Abstraction**: `AsyncIO` trait with platform-specific backends

| Platform | Backend | Features |
|----------|---------|----------|
| Linux | `UringIO` | io_uring, O_DIRECT, 64-entry queue |
| Other | `PosixIO` | `spawn_blocking()`, no O_DIRECT |

**Alignment Requirements**:
- All buffers: 4KB aligned via `AlignedBuffer`
- All offsets: 4KB aligned
- Round-up: `(size + 4095) & !4095`

**Key Trait**:
```rust
pub trait AsyncIO: Send + Sync {
    async fn read_at(&self, buf: &mut AlignedBuffer, offset: u64) -> IoResult<usize>;
    async fn write_at(&self, buf: &AlignedBuffer, offset: u64) -> IoResult<usize>;
    async fn sync(&self) -> IoResult<()>;
}
```

## Query Execution Flow

```
SQL string
 → Parser::parse_one()           [sqlparser crate, MySql dialect]
 → Resolver::resolve()           [name resolution, column indices]
 → TypeChecker::check()          [type validation]
 → LogicalPlanBuilder::build()   [relational algebra tree]
 → Optimizer::optimize()         [predicate pushdown, filter merge]
 → PhysicalPlanner::plan()       [algorithm selection]
 → ExecutorEngine::build()       [construct executor tree]
 → executor.open() / .next() / .close()  [Volcano iteration]
```

**Catalog Lock**: Held during planning phase only. Released before executor I/O.

---

## Module Reference

### `catalog/`

Schema metadata cache. The catalog is an in-memory cache of schema metadata, rebuilt from system tables on startup.

**System Tables** (stored in LSM, replicated via Raft):
- `system.tables` - table definitions
- `system.columns` - column definitions
- `system.indexes` - index definitions
- `system.users` - user accounts (auth)
- `system.grants` - privileges

**Types**:
- `DataType`: Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double, Varchar(n), Text, Blob, Timestamp
- `ColumnDef`: name, data_type, nullable, default, auto_increment
- `TableDef`: name, columns, constraints
- `Constraint`: PrimaryKey, Unique, ForeignKey, Check

**Operations**:
- `get_table()`, `create_table()`, `drop_table()`, `get_column_index()`
- `with_system_tables()`: Bootstrap with system table definitions
- `rebuild_from_storage()`: Reload from system tables (startup)

### `executor/`

Volcano-style query execution. Each operator implements iterator protocol.

**Trait**:
```rust
pub trait Executor: Send {
    async fn open(&mut self) -> ExecutorResult<()>;
    async fn next(&mut self) -> ExecutorResult<Option<Row>>;
    async fn close(&mut self) -> ExecutorResult<()>;
    fn take_changes(&mut self) -> Vec<RowChange>;  // DML row changes (default: empty)
}
```

**Operators**:

| Operator | Algorithm | Notes |
|----------|-----------|-------|
| `TableScan` | Sequential scan | Pushed-down filter, MVCC visibility |
| `PointGet` | PK equality lookup | O(log n) direct key access |
| `RangeScan` | Bounded PK range | Start/end keys, inclusive flags, remaining filter |
| `Filter` | Predicate evaluation | Loop until match or exhausted |
| `Project` | Expression evaluation | Transforms columns |
| `NestedLoopJoin` | Nested loop | Materializes right side (fallback) |
| `HashJoin` | Hash equi-join | O(n+m), builds hash on right side |
| `HashAggregate` | Hash grouping | In-memory HashMap |
| `HashDistinct` | Hash dedup | In-memory HashSet |
| `Sort` | In-memory sort | Materializes all rows |
| `Limit` | Row counting | Offset + limit support |
| `SingleRow` | Constant eval | Expressions with no FROM clause |
| `Insert` | DML | Collects RowChanges via `take_changes()` |
| `Update` | DML | Scan + modify, collects RowChanges |
| `Delete` | DML | Scan + tombstone, collects RowChanges |
| `CreateTable` | DDL | System table writes |
| `DropTable` | DDL | System table deletes |
| `CreateIndex` | DDL | Index metadata + backfill |
| `DropIndex` | DDL | Index metadata removal |
| `CreateUser` | Auth | `system.users` insert |
| `AlterUser` | Auth | `system.users` update |
| `DropUser` | Auth | `system.users` + `system.grants` delete |
| `Grant` | Auth | `system.grants` insert |
| `Revoke` | Auth | `system.grants` delete |
| `SetPassword` | Auth | `system.users` update |
| `ShowGrants` | Auth | `system.grants` query |

**Expression Eval** (`eval.rs`): Recursive evaluation of `ResolvedExpr` against `Row`. Handles literals, column refs, binary ops, functions, IS NULL, IN list, BETWEEN.

**Accumulators**: Count, Sum, Avg, Min, Max

### `io/`

Cross-platform async I/O with priority scheduling.

**Architecture**:
```
                    ┌─────────────────┐
                    │  LSM / Raft     │
                    └────────┬────────┘
                             │ IoContext (urgency + kind)
                    ┌────────▼────────┐
                    │  I/O Scheduler  │  Backpressure, metrics
                    └────────┬────────┘
              ┌──────────────┴──────────────┐
              │                             │
     ┌────────▼────────┐           ┌────────▼────────┐
     │    UringIO      │           │    PosixIO      │
     │   (Linux)       │           │   (non-Linux)   │
     └─────────────────┘           └─────────────────┘
```

**Files**:
- `traits.rs` - `AsyncIO`, `AsyncIOFactory` traits
- `aligned_buffer.rs` - Page-aligned memory allocation
- `scheduler/` - Priority scheduling, backpressure, metrics
- `uring.rs` - Linux io_uring backend
- `posix_aio.rs` - POSIX fallback

**Scheduler** (`scheduler/`):

**Files**: `engine.rs` (core scheduler), `backpressure.rs` (saturation tracking), `batcher.rs` / `batch.rs` (batch submission), `limiter.rs` (rate limiting), `throughput.rs` (throughput tracking), `metrics.rs` (I/O metrics), `config.rs` (tuning parameters).

Two-dimensional I/O context:
```rust
pub enum Urgency {
    Critical,    // Blocks COMMIT (Raft vote, txn durability)
    Foreground,  // User query latency (SLO-bound)
    Background,  // Flush, compaction - deferrable
}

pub enum OpKind { Read, Write, Sync }

pub struct IoContext { urgency: Urgency, kind: OpKind }
```

Backpressure behavior:
- Critical: Always processed immediately
- Foreground: Delayed if backpressure saturated
- Background: Delayed if any backpressure

`ScheduledIOFactory` wraps any `AsyncIOFactory` to add scheduling.

**Backends**:

| Platform | Backend | Features |
|----------|---------|----------|
| Linux | `UringIO` | io_uring, O_DIRECT, 64-entry queue |
| Other | `PosixIO` | `spawn_blocking()`, no O_DIRECT |

**AlignedBuffer**:
- `Layout::from_size_align()` for 4KB alignment
- Manual `NonNull<u8>` management
- Implements `Send + Sync`

### `init/`

Initialization module (`roodb_init` binary). Run before first server startup.

- Creates root user with SHA1(SHA1(password)) hash in `system.users`
- Grants ALL PRIVILEGES on `*.*` with GRANT OPTION in `system.grants`
- Writes schema version marker to storage
- Direct storage writes (bypasses Raft — pre-cluster bootstrap)
- Password via env: `ROODB_ROOT_PASSWORD` (direct) or `ROODB_ROOT_PASSWORD_FILE` (Docker secrets)

### `planner/`

Query planning and optimization.

**Logical Plan** (`logical/mod.rs`):
```rust
pub enum LogicalPlan {
    Scan { table, columns, filter },
    Filter { input, predicate },
    Project { input, expressions },
    Join { left, right, join_type, condition },
    Aggregate { input, group_by, aggregates },
    Sort { input, order_by },
    Limit { input, limit, offset },
    Distinct { input },
    Insert, Update, Delete,
    CreateTable, DropTable, CreateIndex, DropIndex,
}
```

**Build Order** (SELECT):
1. FROM → `Scan`
2. JOIN → nested `Join` nodes
3. WHERE → `Filter`
4. GROUP BY → `Aggregate`
5. HAVING → `Filter` (post-aggregate)
6. SELECT → `Project`
7. DISTINCT → `Distinct`
8. ORDER BY → `Sort`
9. LIMIT → `Limit`

**Optimizer Rules**:
- `PredicatePushdown`: Push filters toward scans, merge into scan filter
- `FilterMerge`: Combine consecutive filters with AND

**Physical Plan** (`physical.rs`): Smart algorithm selection with index utilization:
- `Scan` with PK equality filter → `PointGet` (O(log n) lookup via `extract_point_get()`)
- `Scan` with PK range filter → `RangeScan` (bounded scan via `extract_range_scan()`)
- `Scan` (otherwise) → `TableScan`
- `Join` with equi-keys → `HashJoin` (extracted via `extract_equi_keys()`)
- `Join` (fallback) → `NestedLoopJoin`
- `Aggregate` → `HashAggregate`
- `Distinct` → `HashDistinct`

**Cost Model** (`cost.rs`): Estimates operator costs for plan selection.

**EXPLAIN** (`explain.rs`): Query plan visualization.

### `protocol/`

MySQL-compatible wire protocol (branded as RooDB, version `8.0.0-RooDB`).

**Connection Lifecycle**:
1. TCP accept
2. Send `HandshakeV10` (plaintext) with scramble + `CLIENT_SSL` capability
3. Receive `SslRequest` from client
4. TLS upgrade via `acceptor.accept()`
5. Receive `HandshakeResponse41` (over TLS)
6. Verify credentials (`mysql_native_password`)
7. Command loop: read packet → parse → dispatch → respond

**Packet Format**:
```
| Length (3B LE) | Seq (1B) | Payload (up to 16MB-1) |
```

**Commands**:
- `COM_QUERY` (0x03): Execute SQL
- `COM_QUIT` (0x01): Disconnect
- `COM_PING` (0x0e): Health check
- `COM_INIT_DB` (0x02): USE database
- `COM_RESET_CONNECTION` (0x1f): Reset session
- `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE`: Prepared statements

**Additional Files**:
- `prepared.rs` — Prepared statement lifecycle (prepare, execute, close)
- `metrics.rs` — Protocol-level metrics (packets, bytes, errors)
- `starttls.rs` — TLS upgrade handshake
- `types.rs` — Wire type encoding/decoding

**Result Encoding**:
1. Column count packet
2. Column definition packets
3. EOF packet (unless `CLIENT_DEPRECATE_EOF`)
4. Text row packets
5. Final EOF/OK packet

### `raft/`

Distributed consensus and replication via OpenRaft.

**Architecture**:
- SQL writes → RowChanges → Raft propose → Consensus → Apply to LSM
- Raft log IS the WAL (no separate WAL module)
- DDL = DML on system tables (replicated same path)

**Types** (`types.rs`):
```rust
pub type NodeId = u64;

pub enum Command {
    DataChange(ChangeSet),  // Row-level changes from SQL DML/DDL
    Noop,                    // Leader election
}

pub struct ChangeSet {
    pub txn_id: u64,
    pub changes: Vec<RowChange>,
}

pub struct RowChange {
    pub table: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,  // None = DELETE
    pub op: ChangeOp,            // Insert, Update, Delete
}
```

**RaftNode** (`node.rs`):
- `new()`: Create with optional storage engine integration
- `propose_changes()`: Submit SQL changes for replication
- `bootstrap_single_node()`: Auto-elect as leader
- `bootstrap_cluster()`: Initialize with member list
- `start_rpc_server()`: Handle AppendEntries, RequestVote, InstallSnapshot
- `is_leader()`: Check if this node can accept writes

**Storage**:
- `LsmRaftStorage` (`lsm_storage.rs`): Production storage - persists log/state to LSM, rebuilds catalog on apply
- `MemStorage` (`storage.rs`): Test-only in-memory storage
- Log entries stored in LSM, state machine applies changes to storage and rebuilds catalog
- Vote persistence uses `flush_critical()` (Critical I/O priority) to prevent split-brain

**Read Path**: Direct to local LSM (no Raft)
**Write Path**: SQL → Collect RowChanges → Raft propose → Apply to LSM

### `server/`

TCP server and connection management.

**RooDbServer** (`listener.rs`):
```rust
pub struct RooDbServer {
    addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    storage: Arc<dyn StorageEngine>,
    catalog: Arc<RwLock<Catalog>>,
    txn_manager: Arc<TransactionManager>,
    raft_node: Arc<RaftNode>,
    next_conn_id: AtomicU32,
}
```

**`run()` Loop**:
1. Bind `TcpListener`
2. Accept connection
3. Assign connection ID (atomic increment)
4. Clone shared state
5. `tokio::spawn(handle_connection(...))`

**`handle_connection()`** (`handler.rs`):
1. STARTTLS handshake (plaintext → TLS)
2. Create `RooDbConnection`
3. Complete authentication
4. Enter command loop

**Session** (`session.rs`):
- `current_txn`: Active transaction ID (None = autocommit)
- `autocommit`: Default true
- `isolation_level`: Default RepeatableRead
- Transaction control: BEGIN, COMMIT, ROLLBACK, SET AUTOCOMMIT

### `sql/`

SQL parsing and semantic analysis.

**Parser** (`parser.rs`):
- Wraps `sqlparser` crate with `MySqlDialect`
- `parse_one()`: Single statement only
- Converts to internal AST types

**AST** (`ast.rs`):
- `Statement`: CreateTable, Insert, Select, Update, Delete, etc.
- `Expr`: Column, Literal, BinaryOp, UnaryOp, Function, IsNull, InList, Between
- `ResolvedExpr`: Expression with type information attached
- `ResolvedColumn`: Column with table name, index, data type, nullability

**Resolver** (`resolver.rs`):
- Validates table/column existence against Catalog
- Resolves column indices
- Builds `ResolvedStatement`

**TypeChecker** (`type_checker.rs`):
- Validates expression types
- Infers result types for operations

**Privileges** (`privileges.rs`):
- MySQL-compatible privilege checking with hierarchical scope: global (`*.*`), database (`db.*`), table (`db.table`)
- `Privilege` enum: All, Select, Insert, Update, Delete, Create, Drop, Alter, Index, GrantOption
- `HostPattern`: MySQL-style `'user'@'host'` wildcard matching with specificity ranking
- `check_privilege()` / `check_privileges()`: Verify grants from `system.users` / `system.grants`

### `storage/`

LSM-Tree storage engine.

**Architecture**:
```
┌─────────────┐
│  Memtable   │  Active (SkipMap, dynamic flush threshold)
├─────────────┤
│  Immutable  │  Pending flush
├─────────────┤
│    L0      │  Unsorted SSTables
├─────────────┤
│    L1+     │  Sorted, non-overlapping
└─────────────┘
```

**Memtable** (`lsm/memtable.rs`):
- `crossbeam_skiplist::SkipMap<Vec<u8>, Option<Vec<u8>>>` — lock-free concurrent skip list (None = tombstone)
- Flush threshold: `total_cache_bytes / 2` (default 128MB budget → 64MB threshold)
- Atomic size tracking

**SSTable** (`lsm/sstable.rs`):
```
| Data Block 0 (4KB) |
| Data Block 1 (4KB) |
| ...                |
| Index Block (4KB)  |
| Footer (4KB)       |
```

**Block Format** (`lsm/block.rs`):
```
| Num Entries (4B) | Entry... | Padding | CRC32 (4B) |
Entry: | Key Len (2B) | Val Len (2B) | Key | Value |
```
- Tombstone: `Val Len = 0xFFFF`

**Footer**: Index offset, block count, min/max keys, magic (`LSMT`), version

**Operations**:
- `get()`: Check memtable → immutables → SSTables (newest first)
- `put()`: Insert to memtable, trigger flush if needed
- `scan()`: Merge all sources, deduplicate by key, skip tombstones

**Bloom Filters** (`lsm/bloom.rs`): Per-SSTable Bloom filters for accelerating point lookups. Avoids reading SSTables that definitely don't contain a key.

**Block Cache** (`lsm/block_cache.rs`): LRU block cache with dynamic sizing. Part of the unified memory budget (128MB default) shared with memtables. Starts at full budget, shrinks as memtable grows (minimum 4MB).

**Merge Iterator** (`lsm/merge_iter.rs`): Multi-way merge iterator for range scans across memtables and SSTables. Deduplicates by key, resolves to newest version.

**Memory Budget**: Unified 128MB default (`DEFAULT_TOTAL_CACHE_BYTES`) shared between block cache and memtables. Block cache dynamically rebalances as memtable pressure changes.

**Compaction** (`lsm/compaction.rs`):
- L0 trigger: 4 files
- Level size ratio: 10x
- L1 base: 10MB
- Process: Find overlap → Merge → Write new SSTable → Update manifest

**Manifest** (`lsm/manifest.rs`): JSON file tracking SSTable metadata per level. Uses atomic write pattern (temp file → fsync → rename) to prevent corruption.

### `tls.rs`

TLS configuration (mandatory, no plaintext).

**TlsConfig**:
- `server_config`: rustls `ServerConfig` (no client auth)
- `client_config`: rustls `ClientConfig` (for Raft RPC)

**Loading**:
- `from_files()`: Async file read
- `from_pem()`: Parse cert chain + private key via `rustls_pemfile`

**Self-Signed Support**: Adds server cert as trusted root for client config

### `txn/`

MVCC transaction management.

**Transaction** (`transaction.rs`):
```rust
pub struct Transaction {
    pub txn_id: u64,
    pub state: TransactionState,  // Active, Committed, Aborted
    pub isolation_level: IsolationLevel,
    pub read_view: Option<ReadView>,
    pub is_read_only: bool,
}
```

**Isolation Levels**:
- `ReadUncommitted`: See uncommitted changes
- `ReadCommitted`: Fresh snapshot per statement
- `RepeatableRead`: Single snapshot for transaction (default)
- `Serializable`: Not yet implemented

**TransactionManager** (`manager.rs`):
- Monotonic `txn_id` allocation
- Active transaction tracking
- Committed transaction set
- Leader check for writes

**ReadView** (`read_view.rs`) - InnoDB-style visibility:
```rust
impl ReadView {
    pub fn is_visible(&self, row_txn_id: u64) -> bool {
        // Own writes: visible
        // Before min_active: visible (committed)
        // After max_txn_id: invisible (future)
        // In active set: invisible (uncommitted)
        // Otherwise: visible (committed between)
    }
}
```

**MvccStorage** (`mvcc_storage.rs`):
- Row header: `[txn_id:8][roll_ptr:8][deleted:1][data]`
- `get()`: Check visibility, traverse version chain if needed
- `put()`: Log old version to undo, write new with txn_id

**UndoLog**: Version chain storage for MVCC rollback

**Purge** (`purge.rs`): Background garbage collection of old MVCC versions. Periodically removes undo records no longer needed for visibility — safe when no active transaction references the old version. Uses `min_active_txn_id()` to determine the purge horizon.

**TimeoutConfig** (`mod.rs`):
- `idle_in_transaction_timeout`: 10 minutes (600s) — kills idle transactions
- `statement_timeout`: disabled by default — per-statement deadline
- `lock_timeout`: disabled by default — max wait for row locks

---

## Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| Memory budget | 128 MB (default) | `storage/lsm/engine.rs` |
| Memtable flush threshold | `total_cache_bytes / 2` (default 64 MB) | `storage/lsm/engine.rs` |
| Block cache minimum | 4 MB | `storage/lsm/engine.rs` |
| Block cache size | Dynamic (budget minus memtable usage) | `storage/lsm/engine.rs` |
| SSTable block size | 4 KB | `storage/lsm/block.rs` |
| L0 compaction trigger | 4 files | `storage/lsm/compaction.rs` |
| Level size ratio | 10x | `storage/lsm/compaction.rs` |
| io_uring queue depth | 64 | `io/uring.rs` |
| Max packet size | 16 MB - 1 | `protocol/roodb/packet.rs` |
| Page alignment | 4 KB | `io/traits.rs` |
| Raft election timeout | 150-300 ms | `raft/node.rs` |
| Raft heartbeat interval | 50 ms | `raft/node.rs` |
| Idle-in-transaction timeout | 10 min | `txn/mod.rs` |
