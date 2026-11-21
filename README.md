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
of IOPS from NVMe, and batched direct I/O via io_uring. The result is a database
that keeps the hardware busy instead of waiting on locks and syscalls.

## Features

- **io_uring I/O** — direct I/O (no page cache) submitted in batches by reactor threads on Linux; a POSIX worker-pool backend everywhere, chosen at startup
- **Lock-free memtable** — concurrent skip list for zero-contention writes
- **Unified memory budget** — block cache and memtables share a single adaptive pool
- **LSM storage engine** — write-optimized with cascading compaction
- **Raft consensus** — distributed replication via OpenRaft for high availability
- **Full SQL** — parser, query planner with optimizer, Volcano-style executor
- **MySQL-compatible protocol** — connect with `mysql` CLI or any MySQL client library (TLS required)
- **SQL Server protocol (TDS)** — optional second port for TDS clients, over TDS 8.0 or TDS 7.x, always encrypted (see [admin.md](admin.md#sql-server-protocol-tds))

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
cargo test --release
```

## Documentation

| File | Contents |
|------|----------|
| [SQL.md](SQL.md) | Supported SQL |
| [admin.md](admin.md) | Configuration, deployment, users and privileges, backup, troubleshooting |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Building, testing, debugging, source layout, where to start reading |
| [design.md](design.md) | Architecture and per-module reference |
| [LOCKING.md](LOCKING.md) | Locks and lock ordering |
| [mtesting.md](mtesting.md) | MySQL test suite integration |
| [doc/ROADMAP.md](doc/ROADMAP.md) | Work that remains, in order |

## Architecture

The source layout and the path of a statement through the code are in
[CONTRIBUTING.md](CONTRIBUTING.md#source-layout); [design.md](design.md)
describes each module.

**Replication Model**:
- Leader accepts all writes, replicates via Raft
- Replicas serve read-only queries from local storage
- Schema stored in system tables, replicated like data
- Raft log serves as the write-ahead log

The `roodb` binary currently starts as a single-node Raft cluster; starting a
multi-node cluster from the command line is planned (see
[admin.md](admin.md#deployment-modes)).

## Test Suite

Integration tests start the real server binary and drive it through client
libraries: `mysql_async` for the RooDB (MySQL-compatible) protocol and
`tiberius` for TDS, plus hand-built packets for malformed and adversarial
input. Other tests kill and restart the server to check durability, and run
three-node Raft clusters inside the test process. CI runs the tests on Linux,
macOS and Windows, and on Linux with both I/O backends (io_uring and posix).
See [CONTRIBUTING.md](CONTRIBUTING.md#tests).

## Dependencies

Key dependencies:
- `openraft` - Raft consensus
- `sqlparser` - SQL parsing
- `io-uring` - Linux io_uring (Linux only)

## License

MIT
