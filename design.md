# Design Document: Distributed High-Availability SQL Database (Rust)

## Overview
This document outlines the architecture and design of a generalized, distributed SQL database built in Rust. The system is intended to serve as an OS-level database, usable for anything from lightweight embedded workloads to large-scale, high-throughput deployments. It always operates in distributed Raft mode, even when run as a single node. The design emphasizes modularity, observability, performance, and Linux-native resource awareness.

---

## Design Goals
- **General-purpose**: Usable for any SQL workload (OLTP, OLAP, mixed).
- **Modular**: Pluggable storage backends, adaptable execution models.
- **Highly Available**: Built-in distributed consensus with Raft.
- **Linux-Native**: Embraces modern kernel features like `io_uring`, direct I/O, and scheduler introspection.
- **Resource-Aware**: Userland I/O scheduling for fairness, throughput, and latency control.
- **Extensible**: Clean abstractions to support future optimizations (e.g., JIT, columnar formats).

---

## Core Components

### Language & Runtime
- **Language**: Rust (memory safety, concurrency, performance).
- **Async Model**: Tokio or Glommio for scalable, event-driven async I/O.

### SQL Frontend
- **Parser**: [`sqlparser`](https://github.com/sqlparser-rs/sqlparser-rs)
- **Planner/Optimizer**: Custom, rule-based with cost hints and pluggable optimizations.
- **Wire Protocol**: RooDB client protocol for ecosystem compatibility (clients, connectors, drivers).

### Consensus Layer
- **Raft**: Strongly consistent replication across nodes.
  - Single-node deployments use Raft with a no-replica option.
  - Raft drives log replication, transaction durability, and membership changes.

### Storage Engine
- **Primary Model**: LSM-Tree with leveled compaction, bloom filters, and adaptive caching.
- **Secondary Model**: B+ Tree for read-heavy or embedded workloads.
- **Interface**: Pluggable storage backend trait to support engine selection at boot or per-table.
- **Durability**: Write-ahead log integrated with Raft replication.

### Execution Engine
- **Query Planner**: Logical/physical plan separation.
- **Execution Model**: Volcano-style iterator execution with potential for vectorization in future.
- **Join Strategies**: Nested loop (initial), hash join (planned), index join (via statistics).

---

## I/O Subsystem

### I/O Scheduling
- **Userland Scheduler**: Priority queues for I/O classification (foreground reads, Raft log writes, compaction, background flushes).
- **Backpressure**: Token-bucket or leaky-bucket rate control for background jobs.
- **Async I/O**: `io_uring`-based submission queues, batched syscall dispatch.
- **Direct I/O**: Avoids kernel page cache for deterministic flush and read performance.

### Scheduler Detection & Advisories
- Detect Linux I/O scheduler (e.g., CFQ, BFQ).
- Warn user if suboptimal scheduler is detected (recommend `none`, `mq-deadline` for SSDs).

---

## Observability
- Metrics for:
  - Query latency, Raft commit lag, storage I/O ops, compaction stats.
  - I/O scheduling latency and queue depth.
- Logging of slow queries, Raft membership events, storage anomalies.
- Prometheus metrics endpoint and JSON log output for easy ingestion.

---

## Extensibility
- **Storage Engines**: More formats can be added via a clean trait-based engine API.
- **Query Optimizations**: Future support for vectorization, JIT via Cranelift.
- **Columnar Formats**: Planned for OLAP or analytics use cases.
- **Raft Enhancements**: Lease reads, linearizable read optimizations.

---

## Summary
This system balances correctness, flexibility, and performance by integrating modern Linux and distributed systems design principles. It serves as a resilient, general-purpose SQL engine for the modern era, capable of scaling from embedded deployments to distributed clusters, while giving developers precise control over resources, performance, and visibility.

