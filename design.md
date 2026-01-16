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
}
```

**Operators**:

| Operator | Algorithm | Notes |
|----------|-----------|-------|
| `TableScan` | Sequential scan | Pushed-down filter, MVCC visibility |
| `Filter` | Predicate evaluation | Loop until match or exhausted |
| `Project` | Expression evaluation | Transforms columns |
| `NestedLoopJoin` | Nested loop | Materializes right side |
| `HashAggregate` | Hash grouping | In-memory HashMap |
| `HashDistinct` | Hash dedup | In-memory HashSet |
| `Sort` | In-memory sort | Materializes all rows |
| `Limit` | Row counting | Offset + limit support |

**Expression Eval** (`eval.rs`): Recursive evaluation of `ResolvedExpr` against `Row`. Handles literals, column refs, binary ops, functions, IS NULL, IN list, BETWEEN.

**Accumulators**: Count, Sum, Avg, Min, Max

### `io/`

Cross-platform async I/O abstraction.

**Files**:
- `traits.rs` - `AsyncIO`, `AsyncIOFactory` traits
- `aligned_buffer.rs` - Page-aligned memory allocation
- `uring.rs` - Linux io_uring backend
- `posix_aio.rs` - POSIX fallback

**UringIO** (Linux):
- Opens file with `O_DIRECT`
- Submits read/write via io_uring opcodes
- `submit_and_wait(1)` for completion
- Validates alignment before each operation

**PosixIO** (non-Linux):
- Wraps `std::fs::File` in `Arc<Mutex<>>`
- Uses `tokio::task::spawn_blocking()` for sync I/O
- Copies data to avoid lifetime issues with blocking task

**AlignedBuffer**:
- `Layout::from_size_align()` for allocation
- Manual `NonNull<u8>` management
- Implements `Send + Sync`

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

**Physical Plan**: 1-to-1 mapping with algorithm selection:
- `Join` → `NestedLoopJoin`
- `Aggregate` → `HashAggregate`
- `Distinct` → `HashDistinct`

### `protocol/`

MySQL wire protocol implementation.

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

**Result Encoding**:
1. Column count packet
2. Column definition packets
3. EOF packet (unless `CLIENT_DEPRECATE_EOF`)
4. Text row packets
5. Final EOF/OK packet

**Server Version**: `8.0.0-RooDB`

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

**Storage** (`storage.rs`):
- `MemStorage`: In-memory log + state machine with optional LSM integration
- `LogData`: Vote, log entries (BTreeMap by index)
- `StateMachineData`: Applied log ID, membership, storage engine reference
- When entries are applied, changes are written to the LSM storage

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

### `storage/`

LSM-Tree storage engine.

**Architecture**:
```
┌─────────────┐
│  Memtable   │  Active (BTreeMap, ~4MB)
├─────────────┤
│  Immutable  │  Pending flush
├─────────────┤
│    L0      │  Unsorted SSTables
├─────────────┤
│    L1+     │  Sorted, non-overlapping
└─────────────┘
```

**Memtable** (`lsm/memtable.rs`):
- `BTreeMap<Vec<u8>, Option<Vec<u8>>>` (None = tombstone)
- Flush threshold: 4MB
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

**Compaction** (`lsm/compaction.rs`):
- L0 trigger: 4 files
- Level size ratio: 10x
- L1 base: 10MB
- Process: Find overlap → Merge → Write new SSTable → Update manifest

**Manifest** (`lsm/manifest.rs`): JSON file tracking SSTable metadata per level

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

---

## Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| Memtable flush threshold | 4 MB | `storage/lsm/memtable.rs` |
| SSTable block size | 4 KB | `storage/lsm/block.rs` |
| L0 compaction trigger | 4 files | `storage/lsm/compaction.rs` |
| Level size ratio | 10x | `storage/lsm/compaction.rs` |
| io_uring queue depth | 64 | `io/uring.rs` |
| Max packet size | 16 MB - 1 | `protocol/roodb/packet.rs` |
| Page alignment | 4 KB | `io/traits.rs` |
| Raft election timeout | 150-300 ms | `raft/node.rs` |
| Raft heartbeat interval | 50 ms | `raft/node.rs` |
