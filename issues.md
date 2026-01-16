# Known Issues

## Critical

### Row ID collision in cluster
- **Impact:** Data corruption - two nodes can generate same row ID
- **Location:** `insert.rs:22`, `ddl.rs:29`, `auth.rs:24`
- **Details:** Each node has local AtomicU64 counters. In a cluster, nodes can generate identical row IDs.
- **Fix options:**
  - Use node_id prefix: `row_id = (node_id << 48) | local_counter`
  - Use Raft log index as row_id (guaranteed unique)
  - Request row_id allocation from leader via Raft

### No write-write conflict detection
- **Impact:** Lost updates - concurrent updates to same row, last-write-wins silently
- **Location:** No OCC/locking mechanism exists
- **Details:** Two transactions updating the same row will both succeed without detection.
- **Fix options:**
  - Optimistic: Check version in apply(), reject if changed
  - MVCC first-committer-wins with version numbers

### CREATE TABLE race condition
- **Impact:** Duplicate tables possible via concurrent requests
- **Location:** `ddl.rs:120-132` check-then-act pattern
- **Details:** Existence check happens before Raft proposal. Two concurrent CREATEs can both pass check.
- **Fix:** Move existence check into apply() where Raft serializes all proposals.

## Medium

### Isolation level not clearly enforced
- **Impact:** Unclear transaction isolation guarantees
- **Location:** MVCC read_view semantics in `mvcc_storage.rs`
- **Details:** Need to document and verify isolation level (snapshot isolation vs read committed).

### IF NOT EXISTS evaluated locally
- **Impact:** Could succeed on multiple nodes concurrently
- **Location:** `ddl.rs:123-126`
- **Details:** IF NOT EXISTS is checked locally before Raft proposal.
- **Fix:** Evaluate in apply() for consistency.

## Addressed

### DDL not replicated via Raft
- **Status:** Fixed in Phase 2
- **Details:** CREATE/DROP TABLE/INDEX now go through Raft via system tables.

### Executor writes before Raft commit
- **Status:** Fixed in Phase 1+4
- **Details:** Executors now collect changes; apply() writes to storage.

### Snapshot is metadata-only
- **Status:** Fixed in Phase 3
- **Details:** Snapshots now include full user data serialization.

### put_raw() bypass
- **Status:** Fixed in Phase 5
- **Details:** DML executors now require transaction context; put_raw() fallback removed.

### Rollback doesn't go through Raft
- **Status:** Fixed in Phase 6
- **Details:** With Raft-as-WAL, uncommitted changes never reach storage. Rollback just discards buffered changes.
