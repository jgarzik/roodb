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
- **RooDB Client Protocol**: Clients connect via RooDB protocol over TLS
- **Raft Consensus**: OpenRaft for distributed replication
- **Volcano Executor**: Iterator-based query execution model

### Key Types

- `RooDbServer` (`src/server/listener.rs`) - Main server entry point
- `ConnectionHandler` (`src/server/handler.rs`) - Per-connection state machine
- `ExecutorEngine` (`src/executor/engine.rs`) - Query execution
- `LsmEngine` (`src/storage/lsm/engine.rs`) - LSM storage backend
- `Catalog` (`src/catalog/mod.rs`) - Schema metadata

### Query Execution Flow

```
SQL string
  → Parser::parse_one() (src/sql/parser.rs)
  → LogicalPlanBuilder::build() (src/planner/logical/builder.rs)
  → Optimizer::optimize() (src/planner/optimizer/mod.rs)
  → PhysicalPlanner::plan() (src/planner/physical/planner.rs)
  → ExecutorEngine::build() (src/executor/engine.rs)
  → executor.open() / .next() / .close()
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

Test categories: `ddl/`, `dml/`, `queries/`, `types/`, `errors/`, `edge_cases/`, `cluster/`

## Known Limitations

### Platform-Specific Features

- io_uring only available on Linux
- Use `#[cfg(target_os = "linux")]` for io_uring code
- POSIX fallback used automatically on non-Linux

## Development Notes

- Use `--release` mode for Raft cluster tests (timing sensitive)
- Direct I/O requires 4KB-aligned buffers (`AlignedBuffer::page_aligned()`)
- Debug protocol: update wrapper scripts in `/tmp/*.sh`, re-run to minimize prompts
