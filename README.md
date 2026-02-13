# RooDB

The fastest SQL database on a single machine.

RooDB is built to extract every last drop of performance from modern
multi-core CPUs and high-speed storage devices (NVMe SSDs, Optane).
On single-node and leader-replica configurations, nothing should be faster.

## Goals

* **Maximum single-node throughput** — saturate all cores and I/O bandwidth with lock-free data structures, io_uring, and a unified memory budget that adapts to workload in real time
* **General purpose SQL** — not a niche engine; a real database for real workloads, fast out-of-the-box without tuning
* **High availability** — optional Raft replication across leader + replicas, with reads served locally
* **Near-zero config** — self-tuning defaults that just work; no knob-turning required

## Why RooDB?

Most SQL databases were designed in an era of slow disks and single-core CPUs.
RooDB is designed from scratch for today's hardware: dozens of cores, millions
of IOPS from NVMe, and kernel-bypass I/O via io_uring. The result is a database
that keeps the hardware busy instead of waiting on locks and syscalls.

## Features

- **io_uring I/O** — zero-copy, kernel-bypass I/O on Linux; async POSIX fallback elsewhere
- **Lock-free memtable** — concurrent skip list for zero-contention writes
- **Unified memory budget** — block cache and memtables share a single adaptive pool
- **LSM storage engine** — write-optimized with cascading compaction
- **Raft consensus** — distributed replication via OpenRaft for high availability
- **Full SQL** — parser, query planner with optimizer, Volcano-style executor
- **MySQL-compatible protocol** — connect with `mysql` CLI or any MySQL client library (TLS required)

## Quick Start

### Build

```bash
cargo build --release
```

### Initialize and Run

```bash
# Generate TLS certificate
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key -out certs/server.crt \
    -days 365 -nodes -subj "/CN=localhost"

# Initialize database (first time only, idempotent)
ROODB_ROOT_PASSWORD=secret ./target/release/roodb_init --data-dir ./data

# Start server
./target/release/roodb --port 3307 --data-dir ./data --cert-path ./certs/server.crt --key-path ./certs/server.key
```

### Connect with Client

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
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

## Architecture

```
src/
├── catalog/       # Schema catalog (tables, columns, system tables)
├── executor/      # Volcano-style query executor
├── io/            # Cross-platform async I/O (io_uring / POSIX)
├── planner/       # Query planner and optimizer
├── protocol/      # MySQL wire protocol implementation
├── raft/          # Raft consensus layer (OpenRaft)
├── server/        # TCP listener, connection handling
├── sql/           # SQL parsing and AST
├── storage/       # LSM storage engine
├── tls.rs         # TLS configuration
└── txn/           # Transaction management (MVCC)
```

**Replication Model**:
- Leader accepts all writes, replicates via Raft
- Replicas serve read-only queries from local storage
- Schema stored in system tables, replicated like data
- Raft log serves as the write-ahead log

## Test Suite

RooDB includes a comprehensive integration test suite that validates the full SQL stack across 4 configurations:

| Configuration | Cluster | I/O Backend |
|--------------|---------|-------------|
| `single_uring` | 1-node | io_uring (Linux only) |
| `single_posix` | 1-node | POSIX |
| `cluster_uring` | 3-node Raft | io_uring (Linux only) |
| `cluster_posix` | 3-node Raft | POSIX |

Tests use the `mysql` CLI to execute SQL against RooDB's client protocol.

## Known Limitations

## Dependencies

Key dependencies:
- `openraft` - Raft consensus
- `sqlparser` - SQL parsing
- `io-uring` - Linux io_uring (Linux only)

## License

MIT
