# Raft-as-WAL Alignment Plan

## Design Principle
"Raft is our WAL" — all durable writes go through Raft. Leader: read/write SQL. Replicas: read-only SQL.

## Data Replication Model (No Magic)

**How data reaches followers:**
1. Leader executes SQL → collects `RowChange` (contains actual key/value data)
2. Leader proposes `ChangeSet` to Raft → serialized into log entry
3. Raft replicates log entry to followers via `AppendEntries` RPC
4. Follower receives entry → **must call `storage.put()`** to persist data
5. Currently broken: `apply()` skips this step

**How new/stale followers catch up:**
- If log entries available: replay via `apply()`
- If log truncated: `InstallSnapshot` sends LSM data (SSTables)
- Currently broken: snapshot is metadata-only

**No external replication** — we build everything ourselves via Raft primitives.

## Current Issues

| Issue | Location | Severity |
|-------|----------|----------|
| Followers skip DataChange apply | `lsm_storage.rs:374-392` | CRITICAL |
| DDL bypasses Raft | `ddl.rs` - only in-memory Catalog | CRITICAL |
| Snapshot is metadata-only | `lsm_storage.rs:456-457` | CRITICAL |
| Write before Raft commit | DML writes memtable, then proposes | HIGH |
| put_raw() bypasses Raft | `insert.rs:104`, `update.rs:109` | MEDIUM |
| Rollback bypasses Raft | `manager.rs:229-250` | MEDIUM |

---

## Phase 1: Unified Apply Path (Leader + Follower) ✅ DONE

**Problem**: `lsm_storage.rs:apply()` skips DataChange — data never written on commit

**Current broken flow**:
- Leader writes to memtable BEFORE Raft commit
- apply() skips writing (assumes already written)
- Followers never write (they didn't execute SQL)

**Correct flow (Raft-as-WAL)**:
- Leader collects changes, does NOT write to storage
- Propose to Raft, wait for commit
- apply() writes data to storage — **same code path for leader and follower**

**Changes**:
- `apply()`: For each RowChange, call `storage.put(key, encoded_value)` with MVCC headers
- Remove "already written" skip logic (lines 374-392)
- No `is_leader` flag needed — unified path

**Files**: `src/raft/lsm_storage.rs`

**Test**: 3-node cluster: INSERT on leader → SELECT on any node returns data

---

## Phase 2: DDL Through Raft ✅ DONE

**Problem**: CREATE/DROP TABLE only updates in-memory Catalog

**Changes**:
- `ddl.rs`: Inject RaftNode, emit RowChanges for system.tables/columns/indexes
- `ddl.rs`: Call `propose_changes()` — follow `auth.rs` pattern
- `apply()`: On system table changes, rebuild Catalog entry
- `engine.rs`: Pass RaftNode to DDL executors
- `changes.rs`: Add `delete_with_value()` for tracking deletes in apply()
- `mvcc_storage.rs`: Add `scan_raw()` for DDL system table scanning

**Files**: `src/executor/ddl.rs`, `src/executor/engine.rs`, `src/raft/lsm_storage.rs`, `src/raft/changes.rs`, `src/txn/mvcc_storage.rs`

**Test**: CREATE TABLE → restart → table persists; leader CREATE → follower sees table

---

## Phase 3: LSM Snapshot Transfer

**Problem**: `install_snapshot()` only updates metadata (line 456-457). New/stale followers can't receive data.

**Changes**:
- Define snapshot wire format: SSTable files + manifest + Raft metadata
- `build_snapshot()`: Stream LSM files (SSTables) to snapshot payload
- `install_snapshot()`: Receive and install SSTable files, rebuild memtable state
- Add RPC for incremental snapshot chunks (large data)

**Files**: `src/raft/lsm_storage.rs`, `src/raft/network.rs`, `src/storage/lsm/engine.rs`

**Test**: Start blank follower → receives snapshot → can serve reads

---

## Phase 4: Remove Executor Storage Writes (Part of Phase 1) ✅ DONE

**Note**: This is atomic with Phase 1 — both changes required together.

**Problem**: Executors write to memtable before Raft commit

**Changes**:
- `insert.rs`: Remove `mvcc.put()`, keep only `ctx.add_change()`
- `update.rs`: Remove `mvcc.put()`, keep only `ctx.add_change()`
- `delete.rs`: Remove `mvcc.delete()`, keep only `ctx.add_change()`
- `context.rs`: Add write buffer for uncommitted changes (read-your-writes)
- `scan.rs`: Merge write buffer with storage scan for read-your-writes
- `session.rs`: Accumulate changes across explicit transaction statements
- `protocol/roodb/mod.rs`: Propose on COMMIT, not after each statement

**Files**: `src/executor/insert.rs`, `src/executor/update.rs`, `src/executor/delete.rs`, `src/executor/context.rs`, `src/executor/scan.rs`, `src/server/session.rs`, `src/protocol/roodb/mod.rs`

**Test**: Kill leader mid-INSERT → restart → no orphan data; INSERT then SELECT in same txn sees row

---

## Phase 5: Remove put_raw() Bypass

**Problem**: `put_raw()` when txn_context=None bypasses MVCC+Raft

**Changes**:
- Remove put_raw paths in insert.rs:98-105, update.rs:104-110, delete.rs:100-107
- Protocol layer: Always create implicit txn context for DML
- Only bootstrap/init may use raw storage writes (pre-Raft)

**Files**: `src/executor/insert.rs`, `src/executor/update.rs`, `src/executor/delete.rs`, `src/protocol/roodb/mod.rs`

**Test**: Single INSERT without BEGIN still replicates to followers

---

## Phase 6: Rollback Through Raft

**Problem**: `manager.rs:229-250` writes directly to storage

**Changes**:
- Option A: Propose rollback changeset (undo operations) to Raft
- Option B: Mark txn aborted in Raft; MVCC visibility hides uncommitted; GC later

**Recommendation**: Option B simpler — uncommitted writes invisible anyway

**Files**: `src/txn/manager.rs`, `src/raft/types.rs` (add Command::AbortTxn if needed)

**Test**: BEGIN → INSERT → ROLLBACK on leader; follower never sees row

---

## Dependency Order

```
Phase 1 + 4 (Unified Apply + Remove Executor Writes) ← ATOMIC, start here
    ↓
Phase 2 (DDL via Raft) ← Needs apply() working
    ↓
Phase 3 (Snapshot Transfer) ← Needs Phase 1+2 for complete snapshots
    ↓
Phase 5 (Remove put_raw) ← Needs Phase 1
    ↓
Phase 6 (Rollback) ← Needs Phase 1
```

---

## Key Files

| File | Purpose |
|------|---------|
| `src/raft/lsm_storage.rs` | State machine apply(); snapshot build/install |
| `src/raft/node.rs` | Leadership tracking, wire to storage |
| `src/raft/network.rs` | Snapshot transfer RPC |
| `src/executor/ddl.rs` | DDL via Raft |
| `src/executor/auth.rs` | Reference pattern |
| `src/executor/insert.rs` | DML write path |
| `src/executor/context.rs` | Txn write buffer for read-your-writes |
| `src/txn/manager.rs` | Rollback logic |
| `src/storage/lsm/engine.rs` | Snapshot file streaming |

---

## Verification (End-to-End)

1. **Single node**: INSERT → restart → data persists
2. **3-node cluster**: INSERT on leader → SELECT on any replica returns data
3. **New follower join**: Blank node joins → receives snapshot → serves reads
4. **Leader crash**: Leader dies mid-transaction → new leader elected → no orphan data
5. **DDL replication**: CREATE TABLE on leader → all nodes have table
6. **Rollback**: BEGIN/INSERT/ROLLBACK → row never visible on any node

---

## Design Decisions

1. **Snapshot chunk size**: 2MB (matches Linux huge pages)

2. **Compaction during snapshot**: Pin SSTable files during streaming

3. **No undo log for new writes**: With Raft-as-WAL, uncommitted changes stay in buffer until commit. No undo log needed for rollback.

4. **MVCC headers in apply()**: Use `roll_ptr=0` for committed data (simplified model)
