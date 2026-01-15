# RooDB Implementation Plan

## Goal
Build distributed SQL DB from scratch. Start with Raft server, then io_uring, then SQL stack.

## Architecture Summary
```
SQL → Parser → Planner → Optimizer → Physical Plan → Executor → Storage
                                                         ↓
                                           Raft → WAL → io_uring/POSIX
```

---

## Phase 1: Foundation & Raft Server ✓ COMPLETE
**Goal**: Working Raft cluster (3-node) with in-memory log storage. Auto-bootstrap single-node.

### Files:
- `src/lib.rs` - crate root, module declarations
- `src/main.rs` - binary entry point, CLI parsing
- `src/raft/mod.rs` - module root
- `src/raft/types.rs` - type aliases for OpenRaft (NodeId, Entry, etc.)
- `src/raft/node.rs` - `RaftNode` (init, auto-bootstrap single-node as leader)
- `src/raft/storage.rs` - in-memory Raft storage (log, state machine, snapshot)
- `src/raft/network.rs` - Raft RPC network over TLS
- `src/tls/mod.rs` - TLS config (rustls)
- `src/server/mod.rs` - module root
- `src/server/listener.rs` - TCP listener, accept loop

### Tests:
- `tests/raft_cluster.rs` - leader election, log replication, single-node bootstrap

---

## Phase 2: IO Layer (io_uring + POSIX) ✓ COMPLETE
**Goal**: Async IO abstraction for direct IO with 4KB alignment

### Files:
- `src/io/mod.rs` - module root
- `src/io/error.rs` - IO error types
- `src/io/traits.rs` - `AsyncIO` trait (read_at, write_at, sync)
- `src/io/aligned_buffer.rs` - 4KB-aligned buffers for direct IO
- `src/io/uring/mod.rs` - io_uring backend (Linux, `#[cfg(target_os = "linux")]`)
- `src/io/posix_aio.rs` - POSIX async fallback (non-Linux)

### Tests:
- `tests/io_integration.rs` - read/write round-trip, alignment validation

---

## Phase 3: WAL (Write-Ahead Log) ✓ COMPLETE
**Goal**: Durable log using IO layer. Integrate with Raft.

### Files:
- `src/wal/mod.rs` - module root
- `src/wal/error.rs` - WAL errors
- `src/wal/record.rs` - log record format (type, LSN, data, CRC32)
- `src/wal/segment.rs` - segment file management
- `src/wal/file.rs` - WAL file operations using IO layer
- `src/wal/manager.rs` - WAL manager (append, sync, recovery)

### Integration:
- Update `src/raft/storage.rs` to persist via WAL

### Tests:
- `tests/wal_tests.rs` - append, recovery, corruption detection

---

## Phase 4: Storage Engine (LSM-Tree) ✓ COMPLETE
**Goal**: Key-value storage with ordered iteration

### Files:
- `src/storage/mod.rs` - module root
- `src/storage/error.rs` - storage errors
- `src/storage/traits.rs` - `StorageEngine` trait (get, put, delete, scan)
- `src/storage/lsm/mod.rs` - LSM module
- `src/storage/lsm/memtable.rs` - in-memory sorted buffer (BTreeMap)
- `src/storage/lsm/sstable.rs` - sorted string table (immutable on disk)
- `src/storage/lsm/block.rs` - SSTable block format
- `src/storage/lsm/manifest.rs` - level metadata, current state
- `src/storage/lsm/compaction.rs` - leveled compaction
- `src/storage/lsm/engine.rs` - `LsmEngine` impl

### Tests:
- `tests/storage_tests.rs` - CRUD, range scans, compaction, recovery

---

## Phase 5: Catalog ✓ COMPLETE
**Goal**: Schema metadata (tables, columns, indexes)

### Files:
- `src/catalog/mod.rs` - `Catalog` struct, `TableDef`, `ColumnDef`, `IndexDef`

---

## Phase 6: SQL Layer ✓ COMPLETE
**Goal**: Parse SQL, resolve names, typecheck

### Files:
- `src/sql/mod.rs` - module root
- `src/sql/error.rs` - SQL errors
- `src/sql/ast.rs` - internal AST types (wrapper around sqlparser)
- `src/sql/parser.rs` - `Parser::parse_one()` wrapper
- `src/sql/resolver.rs` - name resolution against Catalog
- `src/sql/typecheck.rs` - type checking

### Tests:
- `tests/sql_tests.rs` - parse/resolve/typecheck validation

---

## Phase 7: Query Planner ✓ COMPLETE
**Goal**: Logical→Physical plan transformation with optimizer

### Files:
- `src/planner/mod.rs` - module root
- `src/planner/error.rs` - planner errors
- `src/planner/logical/mod.rs` - logical plan nodes (Scan, Filter, Project, Join, Aggregate)
- `src/planner/logical/expr.rs` - logical expressions
- `src/planner/logical/builder.rs` - `LogicalPlanBuilder::build()`
- `src/planner/physical/mod.rs` - physical plan nodes
- `src/planner/physical/planner.rs` - `PhysicalPlanner::plan()`
- `src/planner/optimizer/mod.rs` - `Optimizer::optimize()`
- `src/planner/optimizer/rules.rs` - optimization rules (predicate pushdown, etc.)
- `src/planner/cost.rs` - cost model
- `src/planner/explain.rs` - EXPLAIN output

---

## Phase 8: Executor (Volcano) ✓ COMPLETE
**Goal**: Iterator-based query execution

### Files:
- `src/executor/mod.rs` - module root, `Executor` trait (open/next/close)
- `src/executor/error.rs` - executor errors
- `src/executor/row.rs` - `Row` type
- `src/executor/datum.rs` - `Datum` enum (Null, Int, Float, String, Bytes, etc.)
- `src/executor/encoding.rs` - row encoding/decoding for storage
- `src/executor/eval.rs` - expression evaluation
- `src/executor/engine.rs` - `ExecutorEngine::build()` builds executor tree from physical plan
- `src/executor/scan.rs` - table scan operator
- `src/executor/filter.rs` - filter operator
- `src/executor/project.rs` - projection operator
- `src/executor/insert.rs` - INSERT operator
- `src/executor/update.rs` - UPDATE operator
- `src/executor/delete.rs` - DELETE operator
- `src/executor/join.rs` - nested loop join
- `src/executor/aggregate.rs` - GROUP BY, aggregates (COUNT, SUM, AVG, MIN, MAX)
- `src/executor/sort.rs` - ORDER BY
- `src/executor/limit.rs` - LIMIT/OFFSET
- `src/executor/distinct.rs` - HashDistinct operator
- `src/executor/ddl.rs` - DDL operators (CreateTable, DropTable, CreateIndex, DropIndex)

### Tests:
- `tests/executor_tests.rs` - unit tests for datum, row, encoding; integration tests for all operators

---

## Phase 9: MySQL Protocol ✓ COMPLETE
**Goal**: MySQL wire protocol (TLS only)

### Files:
- `src/protocol/mod.rs` - module root
- `src/protocol/mysql/mod.rs` - MySQL module, MySqlConnection state machine
- `src/protocol/mysql/error.rs` - protocol errors
- `src/protocol/mysql/packet.rs` - packet read/write (4-byte header)
- `src/protocol/mysql/handshake.rs` - initial handshake sequence
- `src/protocol/mysql/auth.rs` - mysql_native_password auth
- `src/protocol/mysql/command.rs` - COM_QUERY, COM_QUIT, COM_INIT_DB, COM_PING
- `src/protocol/mysql/resultset.rs` - result set encoding
- `src/protocol/mysql/types.rs` - MySQL type mapping
- `src/protocol/mysql/prepared.rs` - prepared statements stub

### Tests:
- `tests/protocol_tests.rs` - packet encoding, handshake, auth, commands, result sets

---

## Phase 10: Server Integration ✓ COMPLETE
**Goal**: Full server tying everything together with STARTTLS

### Files:
- `src/server/handler.rs` - handles STARTTLS handshake, spawns MySqlConnection
- `src/server/session.rs` - session state (connection_id, database, user)
- `src/protocol/mysql/starttls.rs` - STARTTLS handshake for MySQL compatibility

### Key Changes:
- **STARTTLS Protocol**: Plaintext greeting → SSLRequest → TLS upgrade → auth over TLS
- **System Variables**: Mock @@socket, @@max_allowed_packet, etc. for client init
- **No CLIENT_DEPRECATE_EOF**: Use traditional EOF-based protocol for compatibility

### Integration:
- MySQL protocol → SQL parser → Planner → Executor → Storage (LsmEngine)

### Tests:
- `tests/protocol_tests.rs::test_server_integration_e2e` - full E2E with mysql_async

---

## Phase 11: Transaction Manager ✓ COMPLETE
**Goal**: ACID-compliant transaction support with MVCC

### Design Decisions:
- **Default isolation**: REPEATABLE READ (MySQL default)
- **Concurrency**: Full MVCC (InnoDB-style undo log)
- **Row versioning**: Inline TXN_ID + ROLL_PTR, undo log for old versions
- **Timeouts**: PostgreSQL-style (idle_in_transaction, statement, lock)
- **Raft model**: Leader=read/write, Replica=read-only
- **Commit durability**: storage.flush() per commit (fsync)
- **Write conflicts**: First-writer-wins (MySQL behavior)

### Files (Created):
- `src/txn/mod.rs` - module root, error types, TimeoutConfig ✓
- `src/txn/transaction.rs` - Transaction, TransactionState, IsolationLevel ✓
- `src/txn/read_view.rs` - ReadView, MVCC visibility algorithm ✓
- `src/txn/undo_log.rs` - UndoLog, UndoRecord, version chain ✓
- `src/txn/manager.rs` - TransactionManager (begin/commit/rollback) ✓
- `src/txn/purge.rs` - PurgeTask background cleanup ✓
- `src/txn/mvcc_storage.rs` - MVCC wrapper around StorageEngine ✓
- `src/executor/context.rs` - TransactionContext (txn_id + read_view) ✓

### Files (Modified):
- `src/server/session.rs` - transaction state per connection ✓
- `src/server/handler.rs` - pass TransactionManager to connections ✓
- `src/server/listener.rs` - create TransactionManager ✓
- `src/protocol/mysql/mod.rs` - BEGIN/COMMIT/ROLLBACK/SET parsing ✓
- `src/protocol/mysql/error.rs` - transaction error codes ✓
- `src/executor/mod.rs` - export TransactionContext ✓
- `src/executor/engine.rs` - accept MvccStorage + TransactionContext ✓
- `src/executor/scan.rs` - use MvccStorage.scan() with read_view ✓
- `src/executor/insert.rs` - use MvccStorage.put() with txn_id ✓
- `src/executor/update.rs` - use MvccStorage for read/write ✓
- `src/executor/delete.rs` - use MvccStorage.delete() with txn_id ✓
- `src/executor/error.rs` - From<TransactionError> ✓
- `src/wal/record.rs` - added Commit record type ✓

### WAL:
- `Record::commit(lsn, txn_id)` - commit record constructor
- Commit durability via storage.flush() (fsync); WAL commit logging ready but not yet wired

### Tests:
- `tests/protocol_tests.rs::test_transaction_begin_commit` ✓
- `tests/protocol_tests.rs::test_transaction_rollback` ✓
- `tests/protocol_tests.rs::test_autocommit` ✓

---

## Phase 12: Integration Test Suite ✓ COMPLETE
**Goal**: Full SQL test suite across 4 configurations. `cargo test --release` works (no extra args).

### Test Synchronization:
- Global `OnceLock<Mutex>` serializes integration tests ✓
- Each test: acquire lock → start server → run queries → stop server → release lock ✓
- Prevents port/data conflicts ✓

### Test Framework (mysql_async crate):
- `tests/suite.rs` - test harness entry ✓
- `tests/roodb_suite/harness.rs` - server spawn, mysql_async client, **global mutex** ✓
- `tests/roodb_suite/mod.rs` - single_posix config (main test runner) ✓
- `tests/test_utils/mod.rs` - shared utilities ✓
- `tests/test_utils/certs.rs` - TLS cert generation (rcgen) ✓

### Test Categories (132 tests):
- `tests/roodb_suite/ddl/` - CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX ✓
- `tests/roodb_suite/dml/` - INSERT, UPDATE, DELETE ✓
- `tests/roodb_suite/queries/` - SELECT, filters, joins, aggregates, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT ✓
- `tests/roodb_suite/types/` - Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double, Varchar, Text, Blob, Timestamp, NULL ✓
- `tests/roodb_suite/errors/` - syntax errors, table/column not found, type mismatch, duplicate table ✓
- `tests/roodb_suite/edge_cases/` - empty strings, large varchar, special chars, unicode, case sensitivity, boundaries ✓
- `tests/roodb_suite/cluster/` - Raft leader election, replication consistency, follower reads ✓

---

## Testing Philosophy
- **Unit tests**: Pure functions, data structures only. Run in parallel.
- **Integration tests**: End-to-end SQL via mysql_async. Serialize via global mutex.
- Heavy emphasis on integration tests - validate more components at once.

---

## Development Loop (per change)
1. Review this plan
2. Make planned change
3. Add integration test
4. Validate: `cargo build --release && cargo test --release && cargo clippy`
5. Update plan with progress
6. Git commit at end of phase

---

## Verification
```bash
cargo build --release
cargo test --release
cargo clippy --all-targets
```

---

## Decisions Made
- **Storage**: LSM-Tree first, B+Tree later
- **Raft**: Auto-bootstrap single-node as leader
- **Tests**: mysql_async crate, global mutex for serialization
- **Phase order**: Raft first → io_uring → WAL → Storage → SQL stack
