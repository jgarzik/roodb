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
│  │ I/O threads                 │    │
│  │ - io_uring reactors (Linux) │    │
│  │ - or posix worker pool      │    │
│  └─────────────────────────────┘    │
└─────────────────────────────────────┘
```

**Tasks vs Threads**: Tasks are lightweight async coroutines (green threads), not OS threads. Thousands of connections share a handful of OS threads. Each connection is one task.

**Connection threads** (`server/conn_threads.rs`): connections are served on threads of their own, one per core by default (`--connection-threads`), each running an async runtime with its own I/O event loop. A new connection goes to the thread with the fewest open connections and stays there. With one runtime for everything there is one event loop, and for short requests it hands work to the runtime's threads almost one at a time: a few cores were busy however many clients were connected (point selects: about 0.4 million a second at 64 clients, against 1.5 million with a loop per thread). Raft, the storage engine's background work and the accept loop stay on the main runtime; file I/O goes to the I/O engine's own threads from wherever it is asked for. Connections that share a thread take turns between awaits: a statement that computes for long delays the others on its thread, not those on other threads.

**Yielding Behavior**:

| Phase | Yields? | Notes |
|-------|---------|-------|
| Network read/write | Yes | Async I/O |
| Storage read/write | Yes | Async I/O (io_uring reactors or posix worker pool) |
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

**Statement failures**: a statement that fails is reported to the client and the session goes on (`QueryEngine::handle_statement`, the one entry both protocols use for a client's statement); only a failure of the connection itself (I/O, framing, TLS, login) ends it. `handle_query` underneath returns a statement's failure to its caller, which a procedure running the statements of its body needs.

**Per-Connection State**:
- `Session` - user, current database (none until one is selected, on the MySQL protocol), transaction state, isolation level
- `PacketReader`/`PacketWriter` - protocol framing
- Connection ID (atomic counter)

## I/O Patterns

**Abstraction**: one `IoEngine`, backend chosen at startup (see `io/` below)

| Backend | Where | Features |
|---------|-------|----------|
| `uring` | Linux (default) | io_uring reactors, batched submission, registered files, O_DIRECT |
| `posix` | Everywhere | worker pool, positional I/O, direct I/O where available |

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
protocol front-end (RooDbConnection / TdsConnection)
 → QueryEngine::handle_query() / execute_prepared()  [src/server/query/engine.rs]
 → Parser::parse_one()           [sqlparser crate, MySql dialect]
 → Resolver::resolve()           [name resolution, column indices]
 → TypeChecker::check()          [type validation]
 → LogicalPlanBuilder::build()   [relational algebra tree]
 → Optimizer::optimize()         [predicate pushdown, filter merge]
 → PhysicalPlanner::plan()       [algorithm selection]
 → ExecutorEngine::build()       [construct executor tree]
 → executor.open() / .next() / .close()  [Volcano iteration]
 → ResponseSink (rows, OK, errors) → protocol encoder  [RooDbSink / TdsSink]
```

**Catalog Lock**: Held during planning phase only. Released before executor I/O.

---

## Module Reference

### `catalog/`

Schema metadata cache. The catalog is an in-memory cache of schema metadata, rebuilt from system tables on startup.

**System Tables** (stored in LSM, replicated via Raft):
- The system tables are the tables of the database `mysql` (`catalog/system_tables.rs`); `information_schema` exists as a name and holds nothing. Every other database is a row of `mysql.databases`.
- A table, view, routine or trigger belongs to a database. The catalog, the plans and the executors name a table by `TableName { database, table }` (`catalog/name.rs`), never by one string: a dot in either part is part of that name. The resolver turns a name as written into a `TableName` (`Resolver::canonical`): the session's current database for a name without one (error 1046 if there is none), error 1049 for a database that does not exist. A view's query is resolved in the view's database; a procedure's or trigger's body is run with the session's database set to its own for the duration.
- `mysql.tables` - one record per table, keyed by its database and name: its definition (columns, constraints) and its indexes, whole (`catalog/store.rs`). A change to a table replaces that one record, so it is applied whole or not at all; the state machine brings the catalog up to date from the record as each log entry is applied, with nothing else to read and nothing that can fail. Definitions are validated before they are proposed (`Catalog::validate_table`). A replacement must be the next version (`TableDef::version`) of the record that is stored, and a drop must carry the stored record's ID; otherwise the change is refused. So two statements that each read a table and changed it cannot silently undo one another, and since the check reads what is stored and not the in-memory catalog (which also holds each node's sessions' temporary tables), every node decides the same.
- Every system table's rows are keyed by what they describe (an account by name and host, a view by its name, ...) and carry the version header every stored row has, from initialisation on. `catalog/store.rs` is the one place that builds their keys and reads them; a second row with the same key is refused when it is applied.
- `mysql.databases` - databases
- `mysql.views`, `mysql.procedures` (stored procedures and functions), `mysql.triggers`
- `mysql.table_statistics` - optimizer statistics
- `mysql.users`, `mysql.grants`, `mysql.roles`, `mysql.role_grants` - accounts and privileges

The system tables are the only durable copy of the schema. A schema statement
writes rows to them through Raft; the catalog changes when those rows are
applied (on every node), and is rebuilt in full from them at startup. Stored
functions are copied from the catalog into each session when the catalog's
schema version changes.

Temporary tables are the exception: their definition is registered in the
catalog of the node the session is on, and their rows are kept in that
session's memory (`server/temp_store.rs`), never in the log or the LSM. To
every other session the table does not exist: statements naming it fail with
"table doesn't exist". (The name is still taken, so two sessions cannot use
the same temporary table name at once.) The check is in the `Resolver`,
which is given the session's own temporary tables and finds no others.
Triggers and views cannot be created on temporary tables, nor triggers on
system tables.

**Types**:
- `DataType`: Boolean, TinyInt, SmallInt, Int, BigInt, BigIntUnsigned, Float, Double, Decimal, Varchar(n), Text, Blob, Bit(n) (n = 1..64, held as a u64), Timestamp, Json, Geometry
- `ColumnDef`: name, data_type, nullable, default, auto_increment
- AUTO_INCREMENT (`txn/auto_inc.rs`): one counter per table. The primary holds each table's next value in memory and inserts take values from it. Every inserted row of such a table carries the counter's value after it (`RowChange::auto_inc`); applying the row raises the stored counter (key `autoinc:<table>`) to that, so the counter is replicated and there after a restart. The memory copy is loaded from the stored one on first use, and dropped when the table is dropped or the node becomes primary.
- `TableDef`: name, columns, constraints
- `Constraint`: PrimaryKey, Unique, ForeignKey, Check

**Operations**:
- `get_table()`, `create_table()`, `drop_table()`, `get_column_index()`
- `with_system_tables()`: Bootstrap with system table definitions
- Rebuilt from the system tables by the Raft state machine (`rebuild_catalog_from_system_tables()` in `raft/lsm_storage.rs`) at startup, and updated there as entries are applied

### `executor/`

Volcano-style query execution. Each operator implements iterator protocol.

**Row changes** (`raft/changes.rs`): what a statement proposes and the log carries is a list of row changes: a key, a value, what to do. A change names no table: its table is the ID its key begins with. The state machine tells the system's own tables by their fixed IDs, and looks a table's name up in the catalog only to word a message.

**Keys** (`encoding.rs`): every table has a number, its ID, given once when it is created (`Command::AllocIds`, one counter for the cluster; the system tables have fixed IDs below 1024, and a session's temporary tables take IDs from a range of their own that never leaves the process). A table's keys are `t`, the table ID, an index ID (0 for the rows themselves), then the key values: so a table's rows are together, a renamed table keeps them, and a table created under a dropped table's name has none of the old one's. The `AUTO_INCREMENT` counter is stored under the table's ID too.

**Values in keys** (`key.rs`): storage compares keys as bytes, so each value of a key is written as bytes that sort as the values do: a presence byte (`NULL` first), then integers big-endian with the sign bit flipped, floats by their bits arranged to sort, decimals as sign, magnitude and digits (the same bytes whatever the scale), strings and bytes escaped and terminated so that no value's bytes begin another's. A part can be descending (bytes inverted), a prefix of the value, and strings pass through a `Collation` (binary, for now). A value looked up by key is first put in the key column's type (`for_lookup`): `2.5` against an integer key is no key at all for `=`, and the next whole key for a range bound.

**Rows by column ID** (`rowcodec.rs`; not in use yet: rows are still stored by position, `encode_row`/`decode_row` in `encoding.rs`): a column has a numeric ID that it keeps for as long as it exists and that no later column of the table is given, and a stored row holds its values beside their columns' IDs: a format byte (`0x01`), the number of values, then for each value its column's ID and the value (type tag and payload, as in the positional format, with a tag of its own for a geometry). The count and the IDs are varints (a `u32`, seven bits a byte, shortest form); values are in ascending order of ID, so a row has one encoding. A row is read against the table's columns as they are now (`RowShape`, built once per table definition), in one pass over the values and the columns: a value whose ID the table no longer has is passed over (dropped column), a value goes to wherever its column now stands (reordered columns), and a column the row holds nothing for reads as that column's `missing` value: `NULL`, or the default the column was added with when the table already had rows. So that the absence of a value never means two things, a column whose `missing` is `NULL` stores a `NULL` as nothing at all, and a column whose `missing` is not `NULL` stores every value, a `NULL` as a value with the NULL tag; a column's `missing` therefore stays as it was set unless the table's rows are rewritten. `decode_columns` reads chosen columns and steps over the other values without building them. A value comes back in the type it was stored with; changing a column's type where values must be converted rewrites the rows. Bytes that are not a row (cut short, bytes left over, IDs not ascending, unknown tag or format) are an error.

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
| `CreateUser` | Auth | `mysql.users` insert |
| `AlterUser` | Auth | `mysql.users` update |
| `DropUser` | Auth | `mysql.users` + `mysql.grants` delete |
| `Grant` | Auth | `mysql.grants` insert |
| `Revoke` | Auth | `mysql.grants` delete |
| `SetPassword` | Auth | `mysql.users` update |
| `ShowGrants` | Auth | `mysql.grants` query |

**Expression Eval** (`eval.rs`): Recursive evaluation of `ResolvedExpr` against `Row`. Handles literals, column refs, binary ops, functions, IS NULL, IN list, BETWEEN.

**Accumulators**: Count, Sum, Avg, Min, Max

### `io/`

The block I/O layer. RooDB does its own file I/O instead of relying on the
kernel page cache: files are opened for direct I/O and every request goes
through one `IoEngine` created at startup.

**Architecture**:
```
        LSM (SSTables, compaction)            tasks on any thread
                    │  File::read_at / write_at / sync_data (+ Urgency)
           ┌────────▼────────┐
           │    IoEngine     │   backend chosen at startup (--io-backend)
           └───┬─────────┬───┘
     ┌─────────▼───┐ ┌───▼──────────┐
     │ uring       │ │ posix        │
     │ reactors    │ │ worker pool  │
     └─────────────┘ └──────────────┘
```

**Files**:
- `engine.rs` - `IoEngine`, `File`, `OpenOptions`, `Urgency`, `IoStats`
- `uring.rs` - io_uring backend (Linux)
- `posix.rs` - worker-pool backend (all platforms)
- `aligned_buffer.rs` - page-aligned buffers

**Backends**:

| Backend | Where | How |
|---------|-------|-----|
| `uring` | Linux, default | A few reactor threads, each owning one ring. Requests from all tasks are queued to a reactor, which submits everything pending with one system call. Files are registered in the ring; a write and its sync are linked into one request. `O_DIRECT`. |
| `posix` | Everywhere; fallback when io_uring is unavailable | Worker threads doing positional reads and writes. `O_DIRECT` on Linux, `F_NOCACHE` on macOS, buffered on Windows. |

`--io-backend auto` (the default) uses `uring` when the kernel will create a
ring and otherwise falls back to `posix` with a warning.

**Requests**:
- Buffers are passed by value and handed back on completion. A submitted
  request keeps its buffer and file alive until the kernel is finished, even if
  the caller stops waiting.
- Reads fill the buffer unless the file ends first; writes are complete or an
  error. Write lengths are rounded up to a page and zero padded.
- Offsets and lengths are multiples of 4 KiB.

**Urgency**:
```rust
pub enum Urgency {
    Critical,    // a commit is waiting: Raft log and vote
    Foreground,  // a client query is waiting
    Background,  // flush, compaction
}
```
More urgent requests are submitted first. Background requests are capped per
reactor so they cannot fill a ring.

### `init/`

Initialization module (`roodb_init` binary). Run before first server startup.

- Creates no databases: a new server has `mysql` and `information_schema` only
- Creates root user with SHA1(SHA1(password)) hash in `mysql.users`
- Grants ALL PRIVILEGES on `*.*` with GRANT OPTION in `mysql.grants`
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
- `LogStore` (`log_store.rs`): the Raft log on disk, and the server's write-ahead log. Preallocated segment files under `raft/`, written with direct I/O. Log entries, the vote, truncations, the purge point and the commit point are checksummed records in one stream.
  - One writer task writes everything queued with a single request and then completes the waiting appends: commits that arrive together share a write, and with `--durability fsync` the flush too.
  - Recovery replays the segments; in the newest one the first record that fails its checksum is the end of the log.
  - A spare segment is kept ready and purged segments are recycled.
- `LsmRaftStorage` (`lsm_storage.rs`): the state machine. Applies committed entries to the LSM, rebuilds the catalog on apply, and keeps its own position (last applied, membership) in the LSM.

- Cluster membership: every node has an ID, kept in `NODE_ID` in its data directory. The first node is 1; a node started with `--join` asks a member to admit it (`JoinRequest` on the Raft port), the leader allocates the next ID through the log (`Command::AllocNodeId`), adds the node as a learner and promotes it when it has caught up. Peer addresses are part of the replicated membership. A node that is alone runs without timers or elections; they are switched on at run time when a second node is admitted.
- Snapshots: a snapshot's position is what has been flushed, and its contents are a view of the table files at that moment (`StorageEngine::snapshot_view`), which costs nothing to take; a checkpoint is therefore a flush on a cluster node as on a single one. Only when another node needs the contents are they read, a chunk at a time, and streamed to it (`RaftNetworkConnection::send_snapshot`). The receiver writes them straight into table files of their own (`StorageEngine::ingest`) and switches to them with one manifest change (`Inner::replace_all`), so it holds the old state or the new, never a mixture. The contents are the sender's whole keyspace, the state machine's own records included.
- RPC: each node keeps one TLS connection to each peer and sends its requests over it one after another (`RaftNetworkConnection`); a connection the peer has closed is replaced on the next request. A snapshot travels on a connection of its own.
- Roles: the leader is the primary; every other node is a read-only secondary. `RaftNode::watch_leadership` tells the server when the node starts or stops leading: the transaction manager is told, a node that stops leading rolls back its open transactions (`TransactionManager::abort_all`), and a node that starts leading moves its row ID counter past everything it had reserved before. Sessions refuse writes, DDL and locking reads with error 1290 unless the node leads.
- `Command::Seed`: the first entry of a new cluster's log carries what `roodb_init` wrote to storage before there was a log, so that the log accounts for everything in storage and a joining node receives it.

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
5. Hand the connection to a connection thread (`ConnectionThreads::serve`), which runs `handle_connection(...)`

**`handle_connection()`** (`handler.rs`):
1. STARTTLS handshake (plaintext → TLS)
2. Create `RooDbConnection`
3. Complete authentication
4. Enter command loop

**Session storage** (`temp_store.rs`): `SessionStorage` wraps the shared storage engine for one session. Keys of the session's temporary tables are served from an in-memory map (at most 256 MiB per session); every other key goes to the shared engine. The map is dropped when the session ends.

**Shutdown** (`main.rs`): on SIGTERM or SIGINT the server stops accepting connections, shuts Raft down (closing the log), flushes and closes storage, and exits. A second signal exits at once.

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
- `check_privilege()` / `check_privileges()`: Verify grants from `mysql.users` / `mysql.grants`

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

**Writes**: `put`, `delete` and `write_batch` write to the active memtable under a shared lock held for the whole batch; freezing a memtable takes the lock exclusively. A frozen memtable, and therefore every table on disk, holds whole batches only. A batch is a list of `WriteOp`s (`Put`, `Delete`, `ForgetRange`) that take effect in the order listed. Keys are limited to 65,535 bytes and values to 1 GiB, checked before any of a batch is applied.

**Background work**: one task writes frozen memtables out (oldest first) and then compacts. The compaction task also clears away what forgotten ranges leave behind. Writers pause when four frozen memtables are waiting.

**SSTable** (`lsm/sstable.rs`), format 3. Every region is a whole number of 4 KiB pages:
```
| Data block 0   |  one page, or more for an oversized entry
| Data block 1   |
| ...            |
| Index          |  per block: offset, length, first key; then the last key; CRC32
| Bloom filter   |
| Footer (4KB)   |  index and bloom location, counts, CRC32, magic "LSMT", version
```

**Block Format** (`lsm/block.rs`):
```
| Block Len (4B) | Num Entries (4B) | Entry... | Padding | CRC32 (4B) |
Entry: | Key Len (2B) | Val Len (4B) | Key | Value |
```
- Tombstone: `Val Len = 0xFFFFFFFF`
- A block is one page unless a single entry needs more; the index records each block's length.

**Operations**:
- `get()`: Check memtable → immutables → SSTables (newest first)
- `put()`: Insert to memtable; a full memtable is frozen and handed to the background task
- `snapshot_view()`: the table files as they are at one moment, readable in chunks while the engine goes on changing; `ingest()`: a complete replacement of the contents, written as table files and switched to atomically.
- `next_key()` / `prev_key()`: the nearest live key after or before a key, found by asking each memtable and table for its own nearest entry (one block read per table) instead of reading the range.
- `scan()`: Merge all sources, deduplicate by key, skip tombstones. Adjacent blocks are read in runs of up to 1 MiB, all runs in flight together.
- `write_batch()`: several writes as one unit, in order: all of a batch reaches disk or none of it. One of the writes can be `WriteOp::ForgetRange { start, end }`, which forgets every key in `[start, end)` for good; see Forgotten ranges below.
- `get_durable()`: like `get()` but from the tables and the manifest only, which is what a restart would find; writes still in memory, forgotten ranges among them, are not in it.

**Bloom Filters** (`lsm/bloom.rs`): Per-SSTable Bloom filters for accelerating point lookups, sized for the keys the table actually holds. Avoids reading SSTables that definitely don't contain a key.

**Block Cache** (`lsm/block_cache.rs`): LRU block cache with dynamic sizing, split into up to 16 shards with a lock each. Part of the unified memory budget (128MB default) shared with memtables. Starts at full budget, shrinks as memtable grows (minimum 4MB).

**Merge Iterator** (`lsm/merge_iter.rs`): Multi-way merge iterator for range scans across memtables and SSTables. Deduplicates by key, resolves to newest version.

**Memory Budget**: Unified 128MB default (`DEFAULT_TOTAL_CACHE_BYTES`) shared between block cache and memtables. Block cache dynamically rebalances as memtable pressure changes.

**Compaction** (`lsm/compaction.rs`):
- L0 trigger: 4 files
- Level size ratio: 10x
- L1 base: 10MB
- Deepest level: 6
- Process: pick a level over its limit → merge its files with the overlapping files one level down → write one new SSTable → update manifest → delete inputs
- The merge reads its inputs and writes its output about 4 MiB at a time (`merged_chunk`, shared with the snapshot view), so a compaction's memory does not grow with its tables
- Tombstones are carried down and dropped only when no deeper table overlaps the output's key range
- What a forgotten range forgets of an input is left out of the output. When no level is over its limit, a table that may still hold such keys is compacted anyway: all of L0 into L1 if it is in L0, otherwise that table alone, rewritten in its own level (the last level included)

**Manifest** (`lsm/manifest.rs`): JSON file tracking SSTable metadata per level and the forgotten ranges, with a layout version (3). Replaced atomically (temp file → fsync → rename → fsync directory). A directory written with another version is refused.

**Forgotten ranges** (`WriteOp::ForgetRange` in `StorageEngine::write_batch`): a key range `[start, end)` that the engine is told to forget, on the caller's promise that nothing is written there again. It is how a whole table's or index's rows can go without a delete marker per row. Nothing above the storage engine uses it yet.

Forgetting a range is a write of its batch, like the puts and deletes next to it, and obeys the same three rules. It is ordered: it forgets what was written to the range before it, the earlier writes of its own batch included, and nothing written after it (a later write to the range, even later in the same batch, is a new write and is read as usual; callers promise not to make one, but what happens is defined). It is visible when `write_batch` returns. And it is durable exactly when its batch is: with the flush that writes out the memtable holding the batch, never before and never after. That last rule is what the Raft state machine needs. Each log entry is one batch that also writes the position it was applied at, and after a crash the log is replayed from the position storage holds; so what is on disk must be the result of applying the log up to exactly that position. A range that reached disk ahead of its batch would have a replayed entry read rows as absent that were present the first time (a duplicate-key check, a version check), and the entry could be accepted or refused differently.
- *Memory*: the range goes to the active memtable, under the batch's lock. The memtable removes the entries it holds in the range, tombstones too, and keeps the range in a set of its own, in which ranges that overlap or touch are joined (`Memtable::forget_range`). Older frozen memtables are left as they are: each is flushed on its own, and what it holds was written before the range was forgotten.
- *Reads before the flush*: a range held by a memtable hides whatever lies below that memtable: older memtables and all tables. `get` stops at the first memtable that has the key or has forgotten it; `scan`, `next_key` and `prev_key` take from each source only what the memtables above it have not forgotten. The memtable's own entries are never hidden by its own ranges, since any it has there were written afterwards. `get_durable` and `snapshot_view` show what has been flushed, so until the flush they still show the range's keys, as they still show the old value of any key the batch overwrote.
- *Record*: the flush of the memtable adds its ranges to the manifest's `forgotten` list in the same replacement of the manifest that lists the memtable's table (or on their own, if the memtable held nothing else). Memtables are flushed oldest first, so by then everything written before the range is in listed tables. Each range gets the next number from `forgotten_seq`, a counter that never goes back, and the table gets the counter's value after its own memtable's ranges as its `filtered_seq`. The list is kept sorted and disjoint: a new range takes the place of listed ones where it overlaps them, and what is left of those on either side stays under its old number. Before the flush, nothing about the range is on disk: a crash leaves the directory as it was before the batch, at the earlier position, and the replay of the log applies the batch again. Applying a batch again on top of its own result (forgetting a range a second time) lists the range under a new number and changes nothing else.
- *Which tables a listed range applies to*: those with a `filtered_seq` below the range's number, and only those. Such a table was written (flushed, or picked for compaction) before the range was listed, so everything in it predates the range. A table numbered at or above the range holds nothing the range forgets: what the range forgot was left out when it was written, and any key of the range it does hold was written afterwards. This one comparison is what orders a listed range against all other writes, and what tells a table that was rewritten around a range (and still spans it) from one that has yet to be. Memtables are never subject to a listed range: whatever they hold was written after it.
- *Reads after the flush*: `get`, `get_durable`, `scan`, `next_key`, `prev_key` and snapshot views leave out what a listed range forgets of each table; `get` does not read a table the range applies to, and `next_key`/`prev_key` step over a range in one move per source. The check is a binary search of a list that is normally empty.
- *Flush and compaction*: a flush leaves nothing out (no listed range applies to a memtable). A compaction leaves out of its output what the ranges listed when it was picked forget of each input, and the output records the counter's value at that moment as its `filtered_seq`.
- *Space*: the compaction task (woken after a flush, and at startup if ranges are listed; `flush()` runs it before returning) takes tables that lie wholly inside a range that applies to them off the manifest and deletes their files, with no merge, then compacts the tables that may hold forgotten keys among others. Files are retired as compaction retires its inputs: unlisted durably first, then deleted; a file a snapshot view still holds open stays readable through that handle.
- *Retirement*: a range leaves the list once no listed table it applies to overlaps it. The compaction task checks this while holding the compaction lock, inside the change of the manifest that retires the ranges, so that a range listed by a flush at the last moment is judged against the tables as they are then. Memtables and flushes in progress do not matter: all that a range forgets is in listed tables from the moment the range is listed, and what a later flush lists was written after it. After that nothing is left for the range to hide, and the list stays short.
- *Snapshots*: a view shows what the ranges listed when it was taken left of the tables it holds. A view taken before a range was listed - before the range was forgotten, or before that was flushed - is unaffected: it goes on showing all of the range's keys from the files it holds. `ingest()` clears the list, and the memtables with the ranges they hold, when it commits: the incoming contents were produced by a view that had already left out what was forgotten and flushed before it, and anything forgotten after the view's position is forgotten again when the log is replayed from there. Ingested tables get `filtered_seq` 0.
- *Crash safety*: every state a crash can leave is one the engine starts from. Table written, manifest not replaced: the file is unlisted and startup deletes it; the range and the rest of its batch are not on disk. Manifest replaced: the range and its batch are durable together, and startup wakes the compaction task to carry on. Tables unlisted, files not yet deleted: startup deletes them.

**Crash safety**: a table is written and synced, then listed in the manifest, and only then is its source (a memtable, or the tables a compaction replaced) released. At startup, table files the manifest does not list are deleted, and a listed table that is missing or fails its checksums stops startup.

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
- `RepeatableRead`: the default level. Today it also takes a fresh snapshot per statement, so reads behave as under `ReadCommitted`; a single snapshot per transaction is not implemented yet
- `Serializable`: Not yet implemented

**Row locks** (`lock.rs`): `LockManager` holds the locks transactions take on rows: shared/exclusive record locks, gap locks, insert intentions, with FIFO granting, deadlock detection and wait timeouts. It lives in the `TransactionManager`, which releases a transaction's locks when it ends. `UPDATE`, `DELETE` and `INSERT` lock the rows they touch and then read them as they stand now (`MvccStorage::get_current`), so writers wait for each other. Under REPEATABLE READ they also lock the gaps they read (the whole table for a scan; the gap around a missing key for a lookup), which keeps other transactions from inserting there. `SELECT ... FOR UPDATE` / `FOR SHARE` lock the same way where the scan executors read the tables. The version check when a changeset is applied remains as a backstop for rows changed without a lock. The locks are local to the node and are not replicated: they protect changes that are still buffered in a session.

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
