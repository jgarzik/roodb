# CLAUDE.md

Guidance for Claude Code when working with the RooDB codebase.

## Build and Test Commands

```bash
# Build
cargo build --release

# Run all tests
cargo test --release

# Run specific test
cargo test --release test_name

# Run tests in a module
cargo test --release roodb_suite

# Lint
cargo clippy

# Format check
cargo fmt --check
```

## Architecture

RooDB is a distributed SQL database in Rust with these key design decisions:

- **TLS-Only**: All network communication requires TLS (no plaintext)
- **RooDB Client Protocol**: Clients connect via RooDB protocol (MySQL-compatible) over TLS
- **TDS Protocol**: Optional second listener (`--tds-port`) for SQL Server clients; TDS 8.0 (TLS first) and TDS 7.x (TLS tunnelled after PRELOGIN), always encrypted
- **Protocol-neutral query engine**: Both protocols are thin front-ends over `QueryEngine`; they differ only in framing, login and result encoding
- **Raft Consensus**: OpenRaft for distributed replication
- **Volcano Executor**: Iterator-based query execution model

### Key Types

- `RooDbServer` (`src/server/listener.rs`) - Main server entry point; accept loop for both listeners
- `ServerContext` (`src/server/context.rs`) - Shared state handed to every connection
- `handle_connection()` (`src/server/handler.rs`) - RooDB protocol connection handling
- `tds::connection::serve()` (`src/protocol/tds/connection.rs`) - TDS connection handling
- `QueryEngine` (`src/server/query/engine.rs`) - Session state and statement execution for one client
- `ResponseSink` (`src/server/query/sink.rs`) - How the engine reports results; implemented by `RooDbSink` and `TdsSink`
- `server::auth` (`src/server/auth.rs`) - Authentication shared by both protocols
- `ExecutorEngine` (`src/executor/engine.rs`) - Query execution
- `LsmEngine` (`src/storage/lsm/engine.rs`) - LSM storage backend
- `Catalog` (`src/catalog/mod.rs`) - Schema metadata
- `DataType::Bit(u8)` — BIT(M) with M=1..64, stored as u64

### Query Execution Flow

```
protocol front-end (RooDbConnection / TdsConnection)
  → QueryEngine::handle_query() / execute_prepared() (src/server/query/engine.rs)
  → Parser::parse_one() (src/sql/parser.rs)
  → LogicalPlanBuilder::build() (src/planner/logical/builder.rs)
  → Optimizer::optimize() (src/planner/optimizer.rs)
  → PhysicalPlanner::plan() (src/planner/physical.rs)
  → ExecutorEngine::build() (src/executor/engine.rs)
  → executor.open() / .next() / .close()
  → ResponseSink (rows, OK, errors) → protocol encoder
```

## Test Suite

### Integration Tests (roodb_suite)

The primary test infrastructure runs SQL tests via client CLI across 4 configurations:

| Config | Nodes | I/O Backend |
|--------|-------|-------------|
| single_uring | 1 | io_uring (Linux) |
| single_posix | 1 | POSIX |
| cluster_uring | 3 | io_uring (Linux) |
| cluster_posix | 3 | POSIX |

Test categories: `ddl/`, `dml/`, `queries/`, `types/`, `errors/`, `edge_cases/`, `cluster/`, `auth/`, `prepared/`, `tds/`

All integration tests build into the single `suite` test binary. TDS tests use the `tiberius` client over both transports; `tests/roodb_suite/tds/raw.rs` is a hand-rolled TDS client for sending exact bytes.

## Known Limitations

### Platform-Specific Features

- io_uring only available on Linux
- Use `#[cfg(target_os = "linux")]` for io_uring code
- The I/O backend is chosen at startup (`--io-backend auto|uring|posix`, `ROODB_IO_BACKEND`); `auto` falls back to the posix worker pool when io_uring is unavailable

## Development Notes

- Use `--release` mode for Raft cluster tests (timing sensitive)
- Direct I/O requires 4KB-aligned buffers (`AlignedBuffer::page()` or `AlignedBuffer::pages()`)
- Debug protocol: update wrapper scripts in `/tmp/*.sh`, re-run to minimize prompts

## Benchmarking

- **Always run ALL workloads** — never filter to a single workload. Run the full A/B comparison every time.
- Command: `bench/run.sh --table-size=100000 --duration=30`
- After benchmarks complete, show a comparison table of ALL workload results (before vs after, RooDB vs MySQL).
- Use `bench/compare.sh` to compare against baseline when available.

## Code Quality Gates

**All of the following MUST pass with ZERO warnings before committing:**

```bash
cargo build --release    # must compile clean, zero warnings
cargo clippy             # must pass with zero warnings
cargo fmt --check        # must be formatted
cargo test --release     # all tests must pass
```

These are non-negotiable. Do not commit code with warnings.

## Code Policies

- `#[allow(dead_code)]` markers are banned — do not use them
