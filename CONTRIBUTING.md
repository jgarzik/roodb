# Contributing to RooDB

How to build, test, debug and find your way around the code. For internals
see [design.md](design.md) and [LOCKING.md](LOCKING.md); for running a server
see [admin.md](admin.md); for the supported SQL see [SQL.md](SQL.md).

## Build and check

```bash
cargo build --release
cargo clippy
cargo fmt --check
cargo test --release
```

A change is ready when all four pass with no warnings. CI
(`.github/workflows/ci.yml`) runs `cargo fmt --check` and
`cargo clippy -- -D warnings` on Linux, then builds and runs
`cargo test --release` on Linux, macOS and Windows. Linux uses io_uring by
default, so CI runs the tests once more there with `ROODB_IO_BACKEND=posix`.

Every commit also updates the documentation it affects, so that no document
goes stale.

## Source layout

```
src/
├── main.rs        # roodb server binary: flags, startup
├── bin/roodb_init.rs  # roodb_init: first-time initialisation of a data directory
├── catalog/       # Schema catalog (tables, columns, system tables)
├── executor/      # Volcano-style query executor
├── init/          # Initialisation logic used by roodb_init
├── io/            # Direct file I/O: io_uring and posix backends, AlignedBuffer
├── planner/       # Logical plan, optimizer, physical plan, EXPLAIN
├── protocol/      # Wire protocols: roodb/ (MySQL-compatible) and tds/ (SQL Server)
├── raft/          # Raft consensus (OpenRaft), on-disk log, apply to storage
├── server/        # Listeners, authentication, session, query engine (server/query/)
├── sql/           # Parsing, name resolution, type checking, privileges
├── storage/       # LSM storage engine (storage/lsm/)
├── tls.rs         # TLS configuration
└── txn/           # Transactions: MVCC, read views, undo log, purge
```

[design.md](design.md) has a per-module reference.

### Where to start reading

| Item | File | Role |
|------|------|------|
| `RooDbServer` | `src/server/listener.rs` | Server entry point; accept loop for both listeners |
| `ServerContext` | `src/server/context.rs` | Shared state handed to every connection |
| `handle_connection()` | `src/server/handler.rs` | A RooDB (MySQL-compatible) protocol connection |
| `tds::connection::serve()` | `src/protocol/tds/connection.rs` | A TDS connection |
| `QueryEngine` | `src/server/query/engine.rs` | Session state and statement execution for one client |
| `ResponseSink` | `src/server/query/sink.rs` | How the engine reports results; implemented by `RooDbSink` and `TdsSink` |
| `server::auth` | `src/server/auth.rs` | Authentication shared by both protocols |
| `ExecutorEngine` | `src/executor/engine.rs` | Builds the executor tree from a physical plan |
| `LsmEngine` | `src/storage/lsm/engine.rs` | LSM storage backend |
| `Catalog` | `src/catalog/mod.rs` | Schema metadata |

### Path of a statement

Both protocols are thin front-ends over `QueryEngine`. They differ only in
framing, login and result encoding; SQL behaviour lives in the engine.

```
protocol front-end (RooDbConnection / TdsConnection)
  → QueryEngine::handle_query() / execute_prepared()   src/server/query/engine.rs
  → Parser::parse_one()                                src/sql/parser.rs
  → Resolver::resolve()                                src/sql/resolver.rs
  → TypeChecker::check()                               src/sql/typecheck.rs
  → LogicalPlanBuilder::build()                        src/planner/logical/builder.rs
  → Optimizer::optimize()                              src/planner/optimizer.rs
  → PhysicalPlanner::plan()                            src/planner/physical.rs
  → ExecutorEngine::build()                            src/executor/engine.rs
  → executor.open() / .next() / .close()
  → ResponseSink (rows, OK, errors) → protocol encoder
```

### Things that are easy to get wrong

- All network communication is TLS, for clients and for Raft. There is no
  plaintext path.
- File I/O is direct I/O and needs 4 KiB-aligned buffers: use
  `AlignedBuffer::page()` or `AlignedBuffer::pages(n)` (`src/io/aligned_buffer.rs`).
- io_uring exists only on Linux; that code is behind
  `#[cfg(target_os = "linux")]`. The backend is chosen when the process
  starts (`--io-backend auto|uring|posix`, or `ROODB_IO_BACKEND`); `auto`
  falls back to the posix worker pool when io_uring is unavailable. Code must
  work on both backends.
- `#[allow(dead_code)]` is not used in this code base.

## Tests

Each file directly under `tests/` is one test binary.

| Binary | What it covers |
|--------|----------------|
| `suite` (`tests/suite.rs`, `tests/roodb_suite/`) | Server-level integration tests, described below |
| `sql_tests`, `planner_tests`, `executor_tests` | SQL layer, planner and executor as libraries |
| `storage_tests`, `lsm_durability` | Storage engine; what the LSM keeps across a crash and reopen |
| `io_engine` | The block I/O engine, on every backend the platform has |
| `raft_log_store` | The on-disk Raft log, including recovery |

Unit tests live beside the code in `src/`. `tests/test_utils/` holds shared
helpers (certificates, server processes).

### The `suite` binary

```bash
cargo test --release --test suite                    # all of it
cargo test --release --test suite roodb_suite::ddl   # one category
cargo test --release test_name                       # one test, any binary
```

- `ddl/`, `dml/`, `queries/`, `types/`, `errors/`, `edge_cases/`, `prepared/`,
  `auth/`: SQL tests through the `mysql_async` client. They share one server:
  the first test starts a single `roodb` process (after `roodb_init`) with
  both listeners enabled, and every test connects to it. Tests therefore use
  table names of their own. A test that needs a differently configured server
  cannot use the shared one.
- `tds/`: the same server through the `tiberius` client. Every scenario runs
  over both transports, TDS 7.x and TDS 8.0. `tds/raw.rs` is a small
  hand-written TDS client for sending exact bytes (malformed requests, status
  bits, attention signals).
- `protocol_tests.rs`: packet encoding, handshake, authentication and result
  sets of the RooDB protocol.
- `crash.rs`: starts its own server process on its own data directory, kills
  it without warning and checks that acknowledged work is still there.
- `restart.rs`: data written before a restart is usable after it.
- `raft_cluster.rs`, `cluster/`: Raft behaviour (bootstrap, election, log
  replication, follower reads), using `RaftNode` instances inside the test
  process, including three-node clusters.

The `roodb` binary itself starts as a single-node Raft cluster; starting a
multi-node cluster from the command line is planned (see
[admin.md](admin.md#deployment-modes)). No test runs SQL against a multi-node
cluster of server processes.

Always use `--release` for the Raft cluster tests: they are timing sensitive.

The I/O backend used by tests follows `ROODB_IO_BACKEND`. To exercise the
fallback backend on Linux:

```bash
ROODB_IO_BACKEND=posix cargo test --release
```

### MySQL compatibility tests

`tests/mysql_compat/` holds Python runners that drive a live server with the
`mysqltest` client from the MySQL 8.0 test suite. See
[mtesting.md](mtesting.md).

## Debugging

- Logging uses `tracing`; the server reads the filter from `RUST_LOG`, for
  example `RUST_LOG=roodb=debug`.
- `scripts/perf-profile.sh` runs the integration tests under `samply` and
  writes a profile to `profiles/`; `scripts/analyze-profile.sh` extracts the
  hot spots from one (`scripts/analyze-profile.sh [profile.json.gz]`).
  `scripts/perf-profile.sh --help` lists its options.
- [admin.md](admin.md#troubleshooting) covers start-up and connection
  problems.

## Benchmarks

`bench/run.sh` runs sysbench OLTP workloads (`oltp_point_select`,
`oltp_read_only`, `oltp_read_write`, `oltp_write_only`) against RooDB and
against MySQL 8.0 in Docker, and writes a JSON result file under
`bench/results/`. It needs `sysbench`, `openssl` and, for the MySQL side,
`docker`. `bench/compare.sh a.json b.json` compares two result files and
needs `jq`. The server's data directory for a run is created under
`$TMPDIR` (default `/tmp`) and removed afterwards; the tests put their data
there too, so point `TMPDIR` at a volume with room.

```bash
bench/run.sh --table-size=100000 --duration=30
```

Run all workloads rather than a single one, and compare before against after
and RooDB against MySQL.

## Documentation map

| File | Contents |
|------|----------|
| [README.md](README.md) | What RooDB is; quick start |
| [SQL.md](SQL.md) | Supported SQL |
| [admin.md](admin.md) | Configuration, deployment, users and privileges, backup, troubleshooting |
| [design.md](design.md) | Architecture and per-module reference |
| [LOCKING.md](LOCKING.md) | Locks, lock ordering, atomics |
| [mtesting.md](mtesting.md) | MySQL test suite integration and status |
| [TODO.md](TODO.md) | Known open work |
