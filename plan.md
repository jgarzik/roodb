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

## Current Issues (ALL RESOLVED)

| Issue | Status | Phase |
|-------|--------|-------|
| Followers skip DataChange apply | ✅ Fixed | Phase 1 |
| DDL bypasses Raft | ✅ Fixed | Phase 2 |
| Snapshot is metadata-only | ✅ Fixed | Phase 3 |
| Write before Raft commit | ✅ Fixed | Phase 4 |
| put_raw() bypasses Raft | ✅ Fixed | Phase 5 |
| Rollback bypasses Raft | ✅ Fixed | Phase 6 |

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

## Phase 3: LSM Snapshot Transfer ✅ DONE

**Problem**: `install_snapshot()` only updates metadata (line 456-457). New/stale followers can't receive data.

**Solution implemented**:
- Snapshot format: `[count: u64][key_len: u32][key][value_len: u32][value]...`
- `build_snapshot()`: Serialize all user data (exclude `_raft:` prefix keys)
- `install_snapshot()`: Clear user data, deserialize and install entries, rebuild catalog
- `get_current_snapshot()`: Return current state with data
- `rebuild_catalog_from_system_tables()`: Full catalog rebuild after install

**Files**: `src/raft/lsm_storage.rs`

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

## Phase 5: Remove put_raw() Bypass ✅ DONE

**Problem**: `put_raw()` when txn_context=None bypasses MVCC+Raft

**Solution implemented**:
- Removed put_raw() fallback paths from insert.rs, update.rs, delete.rs
- DML executors now require transaction context (error if None)
- Protocol layer already creates implicit txn context for autocommit DML
- Updated unit and integration tests to provide TransactionContext with MVCC-encoded data

**Files**: `src/executor/insert.rs`, `src/executor/update.rs`, `src/executor/delete.rs`, `tests/executor_tests.rs`

**Test**: Single INSERT without BEGIN still replicates to followers

---

## Phase 6: Rollback Through Raft ✅ DONE

**Problem**: `manager.rs:229-250` writes directly to storage

**Solution implemented**:
With Raft-as-WAL, this problem is naturally solved:
- Uncommitted changes are never written to storage
- Changes are buffered in session until COMMIT, then proposed to Raft
- On ROLLBACK, `session.clear_pending_changes()` discards buffered changes
- `manager.rollback()` just removes transaction from active set
- No storage writes or Raft involvement needed for rollback
- Removed dead code from `manager.rs:rollback()` that iterated empty undo log

**Files**: `src/txn/manager.rs`

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
