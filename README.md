# RooDB

A distributed SQL database written in Rust, featuring Raft consensus, MySQL wire protocol compatibility, and high-performance I/O.

## Goals

* Easy, highly available distributed SQL
* General purpose, works for most workloads
* Near-zero config
* Single node or multi-node (leader+replicas) configurations

roodb should be high performance for all but the most massive,
sharded-cluster workloads.

## Features

- **Raft Consensus**: Distributed replication via OpenRaft for high availability
- **LSM Storage Engine**
- **MySQL Wire Protocol**: Connect using standard `mysql` CLI or any MySQL client library (TLS required)
- **SQL Support**: Parser (sqlparser-rs), query planner with optimizer, Volcano-style executor
- **Cross-Platform I/O**: io_uring on Linux, async POSIX fallback on other platforms

## Quick Start

### Build

```bash
cargo build --release
```

### Run Tests

```bash
# Run all tests (single-threaded - see Known Limitations)
cargo test --release

# Run a specific test
cargo test --release test_name

# Lint
cargo clippy --all-targets
```

### Connect with MySQL Client

```bash
mysql -h 127.0.0.1 -P 3307 -u root --ssl-mode=REQUIRED --ssl-ca=ca.pem
```

## Architecture

```
src/
├── catalog/       # Schema catalog (tables, columns)
├── executor/      # Volcano-style query executor
├── io/            # Cross-platform async I/O (io_uring / POSIX)
├── planner/       # Query planner and optimizer
├── protocol/      # MySQL wire protocol implementation
├── raft/          # Raft consensus layer (OpenRaft)
├── server/        # TCP listener, connection handling
├── sql/           # SQL parsing and AST
├── storage/       # B+Tree storage engine
├── tls/           # TLS configuration
├── txn/           # Transaction management
└── wal/           # Write-ahead log
```

## Test Suite

RooDB includes a comprehensive integration test suite that validates the full SQL stack across 4 configurations:

| Configuration | Cluster | I/O Backend |
|--------------|---------|-------------|
| `single_uring` | 1-node | io_uring (Linux only) |
| `single_posix` | 1-node | POSIX |
| `cluster_uring` | 3-node Raft | io_uring (Linux only) |
| `cluster_posix` | 3-node Raft | POSIX |

Tests use the `mysql` CLI to execute SQL against RooDB's MySQL wire protocol.

## Known Limitations

## Dependencies

Key dependencies:
- `openraft` - Raft consensus
- `sqlparser` - SQL parsing
- `io-uring` - Linux io_uring (Linux only)

## License

MIT
