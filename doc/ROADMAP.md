# RooDB Roadmap

Work that remains, in the order it will be done. Finished items are deleted
from this file, not ticked off: when the file is empty, the work is done.
Git history records what was finished and when.

The goal the phases serve: behave like MySQL, to the point of passing the
MySQL test suite, on a storage and replication core built for modern
multi-core machines.

| Code | Phase |
|------|-------|
| PRF | Performance pass |
| IDX | Secondary and unique indexes; ALTER TABLE done properly |
| ODB | ODBC and sqlcmd in CI |
| ACL | Accounts and authorization |
| TXN | Transaction semantics |
| MIG | Data directory format versions and migration |
| WIN | Windows installer and service |
| MTS | MySQL test suite |

---

## PRF — Performance pass

Needs a quiet machine for every measurement.

- Point selects are level with MySQL at 16 and 64 threads (about 2x ahead
  at one thread): find the limit on the read path.
- Reads from table files are about half the speed of reads from memory.
- Commit latency at one thread: the hops from session to Raft to log writer
  to I/O reactor. Registered buffers, reactor wake-up tuning; an own
  proposal batcher above Raft only if measurements call for it.
- `--durability fsync` at 16 and 64 threads.
- Deadlock retries in `oltp_read_write` at 16 threads ranged from 2 to 153
  between runs of builds that should lock alike: find out what decides it,
  and whether locks are held longer than they need be.
- The lock manager is one mutex, and gap locks a linear list. With row
  locking, single-thread write throughput measured about the same to 10%
  lower than before it (runs alternated on a shared machine; reads
  unchanged): find what the lock path costs per statement.
- Compaction writes one output table however large; split the output
  into tables of bounded size.
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

## MIG — Data directory format versions and migration

A data directory written by an earlier build cannot be opened. `MANIFEST`
(version 2), table files (version 3) and log segments (version 1) carry a
version and are refused on a mismatch, with a message that says to delete
the directory. The schema version marker (`m:schema_version`, 1) is written
by `roodb_init` and never compared. Row values, the MVCC row header, row
keys, system-table rows and the bincode records in the Raft log carry no
version at all.

Must be finished before the first installer release in WIN, so that no
upgrade ever needs a dump and reload.

- One data-directory format number, recorded in `MANIFEST` and bumped by
  any change to what is stored: file formats, key layout, row and row-header
  encoding, system-table rows, log records. The per-file versions stay as
  checks of the single files. A written rule in CONTRIBUTING.md: a commit
  that changes stored bytes bumps the number and adds the migration step.
- Check at start, in `LsmEngine::open` and `LogStore::open`, before
  anything is written: equal runs; older is migrated; newer is refused with
  a message that names both versions and the build needed. The schema
  version marker is compared the same way. No message tells the operator to
  delete data.
- Migration steps: one function per format number, from N to N+1, run in
  sequence. A step writes new files beside the old ones and switches with
  one atomic `MANIFEST` replacement, so that a crash during migration
  leaves the old directory or the new one, never a mixture. The log is
  replayed into table files by the old reader before its format changes.
- Readers for old formats: each step keeps the decoder for the format it
  migrates from (table file, log segment, row, system-table row), apart
  from the current one.
- Where it runs: the server migrates at start by default and logs each
  step; `--no-migrate` (config key of the same name) makes it refuse
  instead, for operators who take a backup first. `roodb_init --migrate`
  does the same without starting the server.
- Tests: data directories written by each released format, kept in
  `tests/fixtures/` as archives with the script that made them; each is
  opened, migrated and read back, on both I/O backends. A crash at every
  step of a migration, then a restart. A directory from a newer format is
  refused and left untouched.
- CI: a job that builds the latest release tag, writes data with it, and
  opens that data with the current build.
- admin.md (Data Directory Layout, a new Upgrading section,
  Troubleshooting), design.md (storage format), CONTRIBUTING.md.

## WIN — Windows installer and service

See issue #19 for the problem statement, the acceptance criteria and the
two end-to-end proofs; they are not repeated here. Read it with
`gh issue view 19 --comments` before starting.

How the work is done: development is Claude Code on Ubuntu; Windows exists
only on GitHub-hosted runners. The owner pushes; Claude Code never does,
but may start and re-run runs of the installer workflow (`gh workflow run`,
`gh run rerun`) and reads results with `gh run view --log-failed` and
`gh run download`. Every push is a round trip, so every run must report
everything needed to diagnose it.

**Platform-neutral work, built and tested on Linux first**

- Server start-up as a library function: move the body of `src/main.rs`
  into `src/server/` as a function that takes the settings, a stop request
  and a status sink. The sink is told: starting (with progress during log
  replay), ready (both listeners bound, Raft bootstrapped), stopping,
  stopped (storage closed). The foreground sink logs these. A test asserts
  their order through the real server.
- Config file: `--config` / `ROODB_CONFIG`, TOML, for `roodb` and
  `roodb_init`; one settings struct in `src/config.rs` shared by both.
  Precedence: flag, environment, file, default. Unknown keys are errors.
  Relative paths in the file are relative to the file. Defaults on Linux
  and macOS do not change.
- Log file: `--log-file` (config key too), rotated daily, a bounded number
  kept; without it, the console as today.
- TLS files from `roodb_init`: `--generate-tls` writes a CA, a server
  certificate and a key (names from the config) when none exist, never
  over existing files; key files readable by the owner only (mode 600; on
  Windows an ACL without inherited entries). Operator-supplied PEM files
  work as before.
- Stop, on every platform, one path for SIGTERM, Ctrl-C and service stop:
  stop accepting; close idle sessions; let running statements finish
  within a grace period (config key); roll back and close the rest; shut
  Raft down; flush and close storage. A second stop request exits at once,
  as today. `RooDbServer` keeps track of its connection tasks to do this.
- Tests for each of the above in the `suite` binary, on both I/O backends
  where a server is involved.
- admin.md (Server Configuration, First-Time Initialization, Backup and
  Recovery, Troubleshooting), README.md quick start, design.md, CLAUDE.md
  key types.

**Windows-only code, kept thin**

- `src/service/` (`cfg(windows)`), using the `windows-service` crate:
  `roodb --service` runs under the service control manager; without the
  flag the binary runs in the foreground as today. The status sink maps to
  StartPending with checkpoints and wait hints, Running, StopPending,
  Stopped. Stop and pre-shutdown are accepted; the handler only forwards
  the stop request. A failed start reports Stopped with a service-specific
  exit code.
- Event Log: start, stop and start failure (with the reason) written to
  the Application log under the source `RooDB`. Everything else goes to
  the log file.
- Foreground mode on Windows: Ctrl-Break, console close and shutdown
  request a stop, as Ctrl-C does.
- Tests: `ServerProcess::stop()` and `a_stop_request_shuts_down_cleanly`
  run on Windows too (Ctrl-Break to the server's process group).
- The service does not depend on its working directory: every path comes
  from the config file, whose path is in the service command line.

**Installer**

- `packaging/windows/roodb.wxs`, WiX v7 (pinned version, `-acceptEula
  wix7`), x64, per-machine, no UI beyond the built-in minimal one. Version
  from `Cargo.toml`. Fixed UpgradeCode; `MajorUpgrade`; a downgrade is
  refused with a message.
- Installs `roodb.exe`, `roodb_init.exe`, LICENSE and the Windows chapter
  of the documentation into `%ProgramFiles%\RooDB`, and adds that
  directory to the system `PATH`.
- Creates `%ProgramData%\RooDB` with `data`, `certs`, `logs` and
  `roodb.toml` (absolute paths, `bind = "127.0.0.1"`, the log file). All of
  it permanent and never overwritten: uninstall and upgrade leave it alone.
  ACL: Administrators and SYSTEM full control, the service account modify,
  nobody else.
- Service `RooDB`: account `NT SERVICE\RooDB`, start on demand, command
  line `roodb.exe --service --config <path>`, stopped before files are
  replaced and on uninstall, removed on uninstall. Restart after a crash;
  no restart after a start that failed on configuration. If `ServiceInstall`
  does not take the virtual account, the first CI run shows it; then
  `NT AUTHORITY\LocalService` with a service SID for the ACL.
- Registers the Event Log source. No firewall rule. No secret in the MSI.
- An upgrade keeps the service's start type as the operator set it.

**CI: `.github/workflows/windows-installer.yml`**

- Triggers: pull requests to main, pushes to main, `workflow_dispatch`
  (input `sign`), tags `v*`. No path filter.
- Jobs: build the release binaries on `windows-latest`; build the MSI, and
  a second MSI from the same revision with a lower version; then one job
  per scenario on a fresh runner, `fail-fast: false`.
- Scenarios, written in Rust in the `suite` binary
  (`tests/roodb_suite/windows_installer.rs`, `cfg(windows)`, `#[ignore]`,
  run by name from the workflow; they drive `msiexec`, `sc` and
  `mysql_async`):
  - fresh install, unattended: the service is registered, stopped, set to
    manual; nothing listens; `roodb_init --generate-tls`; start; log in
    and write; stop; start; read; listening sockets are loopback only;
    uninstall; the service and program files are gone; data, certificates
    and config remain; install again and read the same rows;
  - upgrade from the lower-version MSI with the service running: one
    product entry, one service, same rows, same config, edits to
    `roodb.toml` kept;
  - upgrade from the latest released MSI, once a release has one;
  - downgrade refused;
  - start before initialization, and start with a broken config: the
    service ends Stopped with its exit code, and the reason is in the
    Event Log and the log file;
  - a data directory in a newer format is refused and left untouched.
- Every scenario, pass or fail (`if: always()`), uploads: the MSIs;
  `msiexec /l*v` logs of each run; WiX output; `sc qc`, `sc queryex`,
  `sc qfailure`; Application and System event log entries from the run;
  the RooDB log directory; `icacls` of both directories; directory
  listings; the uninstall registry entry; `netstat -ano`; `roodb.toml`.
  Never key files or passwords. A one-line verdict per check goes to the
  job log.
- Signing: Azure Artifact Signing, on tags and on dispatch with `sign`
  set: both executables before packaging, then the MSI; signatures
  verified with `signtool verify /pa` in the same job. Set up by the
  owner, since it cannot be done from here: an Azure subscription, identity
  validation, a signing account and certificate profile, an app
  registration with a federated credential for this repository, and the
  tenant, client, subscription, endpoint, account and profile names as
  repository secrets or variables.
- Release, on a `v*` tag pushed by the owner: all scenarios pass, then the
  signed `roodb-<version>-x64.msi` and its `.sha256` are uploaded to a
  draft release, which the owner reviews and publishes. The first such
  release waits for MIG.
- `ci.yml`: unchanged apart from what the new tests need; its Windows jobs
  keep passing.

**Local checks on Ubuntu, with packages from apt only**

- Before every hand-over, beside the usual gates: `cargo check` and
  `cargo clippy` with `--target x86_64-pc-windows-gnu --all-targets`
  (`gcc-mingw-w64-x86-64`, `nasm`). CI builds the MSVC target; link-level
  differences are left to CI.
- On artifacts downloaded from a run: `msiinfo` and `msiextract`
  (`msitools`) to read the MSI's tables and files; `osslsigncode verify`
  for signatures; `xmllint` for the well-formedness of the `.wxs`.
- Not used: Wine, building the MSI on Linux (`wixl`, WiX under `dotnet`),
  the MSVC target cross-compiled from Linux, remote sessions on runners.
- CONTRIBUTING.md: these checks, the installer workflow and how to read
  its artifacts.

**Documentation**

- admin.md, a Windows chapter for operators: install (interactive and
  `msiexec /qn`), initialize, configure, enable remote access (bind address
  and the firewall rule the operator adds), start, stop, logs and Event
  Log, upgrade, uninstall, what uninstall keeps and how to remove it,
  verifying the signature and checksum.
- README.md: Windows install in the quick start; CLAUDE.md: the service
  module, the installer workflow, the test category.

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

- The functions that read system-table rows (`row_to_index_def` and its
  siblings in `catalog/system_tables.rs`) index into the row without
  checking its length: a malformed row panics the task that applies the
  log. They should refuse the row instead.
- Seen once, in a full run of the tests and not in 25 repeats of the crash
  tests alone: a restarted server (`generated_ids_do_not_repeat_after_a_kill`)
  answered the harness's TCP probe, and the first real connection was then
  reset by the peer, with nothing in the server's log. Find what closed it.
- Cluster: a member of a cluster of several cannot change the address it
  advertises (only a node that is alone can); requests between nodes do not
  check that the node answering is the node that was meant; the first node
  keeps purging its whole log at each checkpoint after others have joined,
  so a secondary that is briefly behind may be sent the whole database
  where a few entries would do.

- **Prepared statements: `SELECT *` column metadata.**
  `handle_stmt_prepare()` derives result columns from the projections in the
  syntax tree only. `SELECT *` (and qualified wildcards) need the catalog to
  resolve column names and types, so `select_column_names()` returns `None`
  and `COM_STMT_PREPARE_OK` reports `num_columns = 0`. `mysql_async`
  tolerates this; the C client library may hang or misbehave. The fix is to
  run the resolver against the catalog at prepare time.
- Reads that are guaranteed current on a follower (linearizable or
  lease-based reads).
- Network I/O through io_uring.
- Scans that stream rows instead of collecting a range in memory.
