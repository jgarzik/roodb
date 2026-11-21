# RooDB Roadmap

Work that remains, in the order it will be done. Finished items are deleted
from this file, not ticked off: when the file is empty, the work is done.
Git history records what was finished and when.

The goal the phases serve: behave like MySQL, to the point of passing the
MySQL test suite, on a storage and replication core built for modern
multi-core machines.

| Code | Phase |
|------|-------|
| LCK | Row locking: close out |
| CLU | Cluster mode in the server binary |
| PRF | Performance pass |
| IDX | Secondary and unique indexes; ALTER TABLE done properly |
| ODB | ODBC and sqlcmd in CI |
| ACL | Accounts and authorization |
| TXN | Transaction semantics |
| MTS | MySQL test suite |

---

## LCK — Row locking: close out

Row, gap and locking-read locks are in and reviewed.

- T-SQL transaction nesting over TDS: a nested `BEGIN TRAN` is refused
  today; it should count levels, with only the outermost commit committing.
- `FOR UPDATE OF` takes one table name; MySQL allows a list.
- Finding the neighbour of a key still steps over storage-level deletions
  one at a time (it now stops at its bound); make it a merge over the
  sources.
- The test server helper occasionally reports the server as started before
  it accepts connections ("connection refused" in a locking test, once).

Benchmark at 1, 16 and 64 threads on a quiet machine, both durability
modes, against MySQL; confirm single-thread throughput did not regress.

## CLU — Cluster mode in the server binary

The binary starts as a single-node Raft cluster. Multi-node Raft runs only
inside the test process.

- Node identity: `--node-id`, a node identity file in the data directory,
  peer addresses carried in the Raft membership.
- Forming and changing a cluster: join as learner, promote, remove;
  persistent RPC connections between nodes.
- Snapshots for a follower that has fallen behind the log: streamed from a
  checkpoint, any size (today a snapshot is one message, limited to 4 GiB).
- Row ID reservations after recovery on a multi-node cluster.
- Row locks and leadership: every open transaction aborts when its node
  stops being leader.
- Test matrix: single node and three nodes, on both I/O backends, through
  the real binary. Follower killed and rejoining; leader killed and failover;
  lagging follower caught up by snapshot.
- admin.md: deployment modes rewritten from what the binary does.

## PRF — Performance pass

Needs a quiet machine for every measurement.

- Point selects are level with MySQL at 16 and 64 threads (about 2x ahead
  at one thread): find the limit on the read path.
- Reads from table files are about half the speed of reads from memory.
- Commit latency at one thread: the hops from session to Raft to log writer
  to I/O reactor. Registered buffers, reactor wake-up tuning; an own
  proposal batcher above Raft only if measurements call for it.
- `--durability fsync` at 16 and 64 threads.
- The lock manager is one mutex, and gap locks a linear list.
- Compaction reads its inputs whole into memory; make it streaming.
- The posix backend on Windows uses buffered I/O.

## IDX — Secondary and unique indexes; ALTER TABLE done properly

`CREATE INDEX` records metadata only. A `UNIQUE` constraint other than the
primary key is not enforced.

- Index entries stored and maintained on every write; `UNIQUE` enforced;
  the planner uses indexes for lookups and ranges.
- Locking on index records and gaps, as InnoDB does it, so that a statement
  that finds its rows through an index locks only those.
- `ALTER TABLE` without corrupting rows, at any table size:
  - a stable table ID in every key, so that renaming a table is a change of
    metadata (today `RENAME TO` leaves the rows behind under the old name);
  - rows that carry their schema version, so that `ADD COLUMN` and
    `DROP COLUMN` need no rewrite (today `DROP COLUMN` shifts values into
    the wrong columns, and `ADD COLUMN` ignores the default for old rows);
  - re-keying when the primary key changes;
  - renaming a temporary table.
- `CREATE INDEX` with a name already taken, and `DROP INDEX` of an index
  that does not exist, report an error.

## ODB — ODBC and sqlcmd in CI

A CI job on a Linux runner that drives the TDS port with Microsoft ODBC
Driver 18 and `sqlcmd`.

## ACL — Accounts and authorization

Only `root` can log in, and `root` bypasses every check.

- Account rows in the same format as every other row (today they are read
  without their row header, so an account created with SQL cannot log in and
  `CREATE ROLE` inserts duplicates).
- `CREATE USER`, `ALTER USER`, `DROP USER`, `SET PASSWORD`, `SHOW GRANTS`,
  roles; `mysql_native_password`; account statements commit implicitly.
- The privilege model: database-level grants that do not depend on the
  session's current database, the full set of MySQL privileges, `SUPER` in
  place of the name `root`.
- Enforcement at one choke point for planned statements, and in every
  statement handled outside the planner (ALTER, views, procedures,
  functions, triggers, databases, `LOAD DATA`, `SET GLOBAL`).
  - System tables are written through SQL statements only.
  - `LOAD DATA INFILE` needs the `FILE` privilege.
  - Locking reads need `SELECT` and one of `UPDATE`, `DELETE`,
    `LOCK TABLES`.
  - `SHOW` output filtered by what the account may see.
  - Definer and invoker rights for views, procedures and triggers.
- admin.md: the User Management and Privilege System sections describe the
  intended interface; rewrite them from what works.

## TXN — Transaction semantics

- `REPEATABLE READ`: one snapshot for the whole transaction. Today every
  statement takes a new one.
- `UPDATE` and `DELETE` find rows the same transaction inserted.
- `INSERT ... SELECT` takes locks and waits for them.
- Savepoints; `COMMIT WORK`, `AND CHAIN`.
- Statements that commit implicitly in MySQL (DDL) do so here.
- `SERIALIZABLE`.
- `LOCK TABLES`, `GET_LOCK`.
- Removal of old row versions and of the markers of deleted rows.

## MTS — MySQL test suite

Run the MySQL test suite shipped with Debian and Ubuntu and fix what it
finds. Known already, from an audit of SQL.md against a running server:

**Wrong results**
- `JOIN ... USING` and `NATURAL JOIN` return a cross join.
- `UNION DISTINCT` drops its right-hand side.
- `WHERE int_col = '1'` matches nothing; `WHERE int_pk = 1.0` matches
  nothing.
- A decimal inserted into an integer column is stored as 0.
- No range or length checks on any type, strict mode or not.
- Any statement containing `@@` is answered as a system-variable query.
- `GROUP_CONCAT` ignores `ORDER BY` and `DISTINCT`; `JSON_OBJECTAGG` returns
  nulls; `PERCENT_RANK`, `CUME_DIST`, `NTH_VALUE` return NULL; a running
  `SUM() OVER (ORDER BY ...)` returns the partition total.
- A column that is neither grouped nor aggregated returns garbage, or fails
  with an index out of bounds.

**Statements**
- `REPLACE` never replaces, and is parsed with a regular expression.
- `UPDATE` and `DELETE` triggers never fire; `BEFORE INSERT` runs after the
  insert; `INSERT ... SELECT` fires nothing.
- `CREATE TEMPORARY TABLE ... AS SELECT` copies no rows.
- Two sessions cannot use the same temporary table name, and a temporary
  table cannot shadow an ordinary one.
- `CREATE TABLE ... LIKE`, and a foreign key to a missing table, create a
  table that cannot be used.
- A table can be created under the name of a view.
- `DEFAULT CURRENT_TIMESTAMP` makes inserts fail.
- Backticks around a table name become part of the name.
- `db.table` is not understood; all databases share one table namespace.
- `LOAD DATA` works for one-column tables only.

**Stored routines**
- Local variables are substituted as text, inside string literals too.
- `CONTINUE HANDLER` cannot set a local variable.
- `CREATE FUNCTION` accepts a name already taken; `IF` in a function body
  returns NULL.

**Session and metadata**
- `SHOW WARNINGS` is always empty; `SHOW VARIABLES` and `SHOW PROCESSLIST`
  are empty.
- `USER()` and `CURRENT_USER()` return an empty user name.
- `COUNT(*)` is labelled `COUNT(NULL)` in result headers.
- `SLEEP()` returns at once.
- Errors raised while rows are being returned come back as 1690.
- `SET` of an unknown variable is accepted silently.

---

## Appendix — Unordered work

Not yet assigned to a phase.

- **Prepared statements: `SELECT *` column metadata.**
  `handle_stmt_prepare()` derives result columns from the projections in the
  syntax tree only. `SELECT *` (and qualified wildcards) need the catalog to
  resolve column names and types, so `select_column_names()` returns `None`
  and `COM_STMT_PREPARE_OK` reports `num_columns = 0`. `mysql_async`
  tolerates this; the C client library may hang or misbehave. The fix is to
  run the resolver against the catalog at prepare time.
- Data directories written by earlier builds cannot be opened; there is no
  migration.
- Reads that are guaranteed current on a follower (linearizable or
  lease-based reads).
- Network I/O through io_uring.
- Scans that stream rows instead of collecting a range in memory.
