# RooDB Administrator's Guide

## Quick Start (Single Node)

```bash
# 1. Generate self-signed TLS certificate
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key -out certs/server.crt \
    -days 365 -nodes -subj "/CN=localhost"

# 2. Initialize database (first time only)
ROODB_ROOT_PASSWORD=changeme ./roodb_init --data-dir ./data

# 3. Start server
./roodb

# 4. Connect via MySQL client
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
```

## Server Configuration

### Command Line Arguments

```
./roodb --port PORT --data-dir PATH --cert-path PATH --key-path PATH --raft-ca-cert-path PATH
```

| Argument | Environment | Default | Description |
|----------|-------------|---------|-------------|
| `--bind` | `ROODB_BIND` | 0.0.0.0 | Address to listen on (client, TDS and Raft ports) |
| `--port` | `ROODB_PORT` | 3307 | Client connection port |
| `--tds-port` | `ROODB_TDS_PORT` | (off) | Also accept SQL Server protocol (TDS) clients on this port |
| `--data-dir` | `ROODB_DATA_DIR` | ./data | Data directory path |
| `--io-backend` | `ROODB_IO_BACKEND` | auto | File I/O backend: `auto`, `uring` or `posix` (see I/O Backend) |
| `--durability` | `ROODB_DURABILITY` | os | When a commit is acknowledged: `os` or `fsync` (see Durability) |
| `--cert-path` | `ROODB_CERT_PATH` | ./certs/server.crt | TLS certificate |
| `--key-path` | `ROODB_KEY_PATH` | ./certs/server.key | TLS private key |
| `--raft-ca-cert-path` | `ROODB_CA_CERT_PATH` | ./certs/ca.crt | CA certificate for Raft mTLS |
| `--raft-port` | `ROODB_RAFT_PORT` | port + 1000 | Port for traffic between the nodes of a cluster |
| `--advertise-host` | `ROODB_ADVERTISE_HOST` | the bind address | Host name or address at which other nodes reach this one. Needed when binding to 0.0.0.0 |
| `--log-budget-mb` | `ROODB_LOG_BUDGET_MB` | 256 | How much log may accumulate before memory is written out to table files and the log shortened. Bounds the log's disk use and the replay work of a restart |
| `--connection-threads` | `ROODB_CONNECTION_THREADS` | 0 | Threads that serve client connections; 0 means one per core. Each connection is served on one of them, the least busy when it connects |
| `--log-segment-mb` | `ROODB_LOG_SEGMENT_MB` | 64 | Size of the files the log is kept in; the log is shortened a whole file at a time. Applies to files created from then on |
| `--join` | `ROODB_JOIN` | (none) | `host:raft-port` of a member of the cluster to join (several, comma-separated). For a node starting on an empty data directory |

### TLS Requirements

RooDB requires TLS for all connections. There is no plaintext mode. The server performs a STARTTLS handshake: initial connection is plaintext only long enough to negotiate TLS upgrade.

**Self-signed certificate generation:**
```bash
openssl req -x509 -newkey rsa:4096 \
    -keyout server.key -out server.crt \
    -days 365 -nodes \
    -subj "/CN=roodb.local"
```

For production, use certificates from a trusted CA or your organization's PKI.

### Raft mTLS (Inter-Node Authentication)

RooDB enforces mutual TLS (mTLS) for Raft inter-node communication. Each cluster node must present a certificate signed by a trusted CA. This prevents unauthorized nodes from joining the cluster.

**Certificate model:**
- Single CA signs all node certificates
- Each node trusts the CA and verifies peer certificates
- Client port (3307): standard TLS (no client cert required)
- Raft port (4307): mTLS (client cert required, verified against CA)

**Generate CA and node certificates for production:**

```bash
# 1. Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -out ca.crt -days 365 -subj "/CN=RooDB CA"

# 2. Generate node key and CSR
openssl genrsa -out node1.key 4096
openssl req -new -key node1.key -out node1.csr -subj "/CN=roodb-node1"

# 3. Sign node certificate with CA
openssl x509 -req -in node1.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out node1.crt -days 365 -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Repeat steps 2-3 for node2, node3, etc.
```

**Starting nodes with mTLS:**
```bash
# Each node uses its own cert/key, same CA
./roodb --port 3307 --data-dir ./data1 --cert-path ./node1.crt --key-path ./node1.key --raft-ca-cert-path ./ca.crt
./roodb --port 3308 --data-dir ./data2 --cert-path ./node2.crt --key-path ./node2.key --raft-ca-cert-path ./ca.crt
./roodb --port 3309 --data-dir ./data3 --cert-path ./node3.crt --key-path ./node3.key --raft-ca-cert-path ./ca.crt
```

**Testing mTLS rejection:**
A node with a certificate signed by a different CA will be rejected by the cluster. This protects against rogue nodes attempting to join.

### Port Configuration

| Port | Purpose |
|------|---------|
| 3307 (default) | Client connections (MySQL protocol over TLS) |
| 4307 (port + 1000) | Raft RPC (inter-node replication) |
| `--tds-port` (off by default) | SQL Server protocol (TDS) clients, always encrypted |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ROODB_ROOT_PASSWORD` | Set root password on first boot |
| `ROODB_ROOT_PASSWORD_FILE` | Read password from file (Docker secrets) |
| `ROODB_BIND` | Listen address (same as `--bind`) |
| `ROODB_PORT`, `ROODB_DATA_DIR` | Same as `--port`, `--data-dir` |
| `ROODB_CERT_PATH`, `ROODB_KEY_PATH`, `ROODB_CA_CERT_PATH` | Same as `--cert-path`, `--key-path`, `--raft-ca-cert-path` |
| `ROODB_TDS_PORT` | Enable the TDS listener on this port (same as `--tds-port`) |
| `ROODB_IO_BACKEND` | File I/O backend: `auto` (default), `uring` or `posix` (same as `--io-backend`) |
| `ROODB_DURABILITY` | When a commit is acknowledged: `os` (default) or `fsync` (same as `--durability`) |

Priority: `ROODB_ROOT_PASSWORD` takes precedence over `ROODB_ROOT_PASSWORD_FILE`.

### I/O Backend

RooDB does its own file I/O with direct I/O and picks a backend when it starts:

| `--io-backend` | Behaviour |
|----------------|-----------|
| `auto` (default) | io_uring if the kernel allows it, otherwise `posix` (a warning is logged) |
| `uring` | io_uring; startup fails if the kernel refuses it |
| `posix` | Worker threads doing positional reads and writes; works everywhere |

io_uring is unavailable on non-Linux hosts, on old kernels, and in containers
whose seccomp profile blocks it (recent Docker releases block it by default). `roodb_init`
accepts the same flag. The backend in use is logged at startup.

### Durability

Every commit is appended to the Raft log before it is acknowledged. `--durability` chooses how far that append must have got:

| `--durability` | A commit is acknowledged when... | Survives |
|----------------|----------------------------------|----------|
| `os` (default) | its log write has completed | the server crashing or being killed |
| `fsync` | its log write has been flushed to stable storage | that, and power loss or a kernel crash |

With `os`, a power failure can lose the last commits that were still in the drive's volatile cache. With `fsync`, commits that arrive together share one flush, so the cost per commit falls as load rises. Raft votes are flushed in both modes.

## SQL Server Protocol (TDS)

RooDB can accept clients that speak TDS, the SQL Server wire protocol, on
a second port. It is off unless `--tds-port` (or `ROODB_TDS_PORT`) is set:

```
./roodb --port 3307 --tds-port 1433 --data-dir ./data --cert-path ./certs/server.crt --key-path ./certs/server.key
```

The TDS port must differ from the client port and the Raft port. Accounts,
databases and data are the same as on the main port.

### Transports and encryption

Both TDS transports are accepted on the one port, and both are always
encrypted with the server certificate:

| Client setting | Transport | Notes |
|----------------|-----------|-------|
| `Encrypt=Strict` | TDS 8.0: TLS first (TLS 1.2 or 1.3, ALPN `tds/8.0`) | Nothing is sent in cleartext. Microsoft drivers require a certificate they can validate in this mode. |
| `Encrypt=True` / `Mandatory` | TDS 7.x: PRELOGIN in cleartext, then TLS 1.2 for the whole session | PRELOGIN carries driver version and flags only, never credentials or SQL. |
| `Encrypt=False` / `Optional` | TDS 7.x | The server still requires encryption for the whole session. |
| No encryption support | refused | The client is told encryption is required and disconnected before login. |

Login-only encryption is not offered. The server reports TDS 7.4 and
product version 16.0 to clients on either transport.

### What works

- SQL authentication (user name and password). Windows/SSPI and Entra
  (federated) authentication are refused.
- SQL batches, including several statements per batch.
- Parameterised statements: `sp_executesql`, `sp_prepare`, `sp_execute`,
  `sp_prepexec`, `sp_unprepare`. Parameter values are bound as typed
  values.
- Transactions through SQL or through the driver's transaction API.
- Query cancellation (attention signal) and connection-pool reset.

### SQL dialect

The SQL accepted is RooDB's own (MySQL dialect), exactly as on the main
port; see [SQL.md](SQL.md). T-SQL is not translated. Only the statements
drivers issue by themselves are understood in addition:

- `BEGIN TRAN`, `COMMIT TRAN`, `ROLLBACK TRAN`, nested as in T-SQL (only
  the outermost `COMMIT` commits), `IF @@TRANCOUNT > 0 ...`,
  `SET IMPLICIT_TRANSACTIONS ON|OFF`
- connect-time session options such as `SET ANSI_NULLS ON`,
  `SET QUOTED_IDENTIFIER ON`, `SET NOCOUNT ON`, which are accepted without
  effect (double quotes still delimit strings, as in MySQL)
- probes: `@@SPID`, `@@TRANCOUNT`, `@@VERSION`, `DB_NAME()`,
  `SUSER_SNAME()`, `SERVERPROPERTY('...')`

### Limitations

- No savepoints: a named `ROLLBACK TRAN name` or `SAVE TRAN` is refused.
- No MARS (multiple active result sets), bulk load, or table-valued,
  XML and `sql_variant` parameters.
- No SQL Server catalog views (`sys.*`), so tools that browse the catalog,
  such as the SSMS object explorer, will not work.
- Cancellation takes effect between result rows; a statement that works
  for a long time before returning its first row finishes that work first.
- Tested with the `tiberius` client on both transports. Microsoft's ODBC,
  .NET and JDBC drivers have not been verified yet.

### Type mapping

| RooDB type | Sent as |
|------------|---------|
| BOOLEAN, BIT(1) | bit |
| TINYINT, SMALLINT | smallint |
| INT / BIGINT | int / bigint |
| BIGINT UNSIGNED | decimal(20,0) |
| FLOAT / DOUBLE | real / float |
| VARCHAR(n ≤ 4000) | nvarchar(4000) |
| longer VARCHAR, TEXT, JSON | nvarchar(max) |
| BLOB, GEOMETRY | varbinary(max) |
| BIT(n) | binary(8) |
| TIMESTAMP / DATETIME | datetime2(3) |
| DECIMAL(p ≤ 38, s) | decimal(p,s) |

Column types of expressions are settled from the first row, so a computed
column may be reported with the type of its values rather than the type
listed here.

## Data Directory Layout

```
data/
├── *.sst              # LSM table files
├── MANIFEST           # Which table files exist, by level
├── LOCK               # Held while a server has the directory open
├── NODE_ID            # This node's ID in its cluster
└── raft/
    ├── log-*.seg      # The Raft log (the write-ahead log), in fixed-size segments
    └── spare.seg      # A prepared segment waiting to be used
```

**Important:** Back up the entire data directory as a unit. `MANIFEST` says which table files are live; the server refuses to start if it is missing while table files are present, or if a file it lists is missing or damaged. The log in `raft/` holds every commit that has not yet been flushed into a table file.

## Deployment Modes

### Single Node

Single-node deployment is production-ready and fully supported via CLI:

```bash
./roodb --port 3307 --data-dir ./data --cert-path ./certs/server.crt --key-path ./certs/server.key
```

The server automatically bootstraps as a single-node Raft cluster, electing itself as leader. All reads and writes are processed locally.

**Use single-node when:**
- Development and testing
- Small deployments where HA is not required
- Simplicity is preferred over fault tolerance

### Multi-Node Cluster

A cluster is a first node, started as above, and further nodes that join it.
A joining node starts with an **empty data directory** (no `roodb_init`) and
the address of the Raft port of any member:

```bash
# first node: initialised with roodb_init, then started as a single node
./roodb --bind 10.0.0.1 --port 3307 --data-dir ./data ...

# second and third node, each on its own machine and empty data directory
./roodb --bind 10.0.0.2 --port 3307 --data-dir ./data --join 10.0.0.1:4307 ...
./roodb --bind 10.0.0.3 --port 3307 --data-dir ./data --join 10.0.0.1:4307 ...
```

The cluster gives the new node its node ID (kept in `NODE_ID` in the data
directory), sends it the data, and makes it a voting member once it has
caught up. From then on the node knows its cluster: on a restart it needs
no `--join`, and the option is ignored if given.

**Requirements:**
- 3 nodes tolerate the loss of 1; 5 tolerate 2.
- Every node needs a certificate signed by the same CA, and that CA's
  certificate (`--raft-ca-cert-path`): nodes authenticate each other with
  mutual TLS.
- Every node must be able to reach every other node's Raft port
  (`--raft-port`, by default the client port + 1000) at the address the node
  advertises: its `--bind` address, or `--advertise-host` when it binds to
  `0.0.0.0` or sits behind another name. A node that would advertise
  `0.0.0.0` is not allowed to join, and a first node that does is told to be
  restarted with `--advertise-host` before it admits anyone (while it is
  alone, restarting it with the option is all it takes).

**Primary and secondaries:**
- The node that leads the cluster is the **primary**: it takes all writes.
  The others are **secondaries**: they apply what the primary commits and
  answer reads from their own state, which can be slightly behind. A
  secondary refuses anything that writes or locks with error 1290, the
  error of a read-only MySQL server. This is the single-primary model of
  MySQL Group Replication; point writers at the primary.
- When the primary fails or is cut off, the remaining nodes elect a new one
  (150-300 ms election timeout, 50 ms heartbeat), provided a majority of the
  nodes is still up. Nothing that was acknowledged is lost. Transactions
  that were open on the old primary are rolled back.
- A node that was down restarts on its data directory with no special
  options, follows the current primary and catches up.
- A node that has been away for longer than the log reaches back, or that
  joins late, is sent a copy of the data instead: read from the sender's
  table files a piece at a time while the sender goes on working, and
  switched to on the receiver in one step once it has all arrived.

**Operating a cluster:**

```sql
-- on any node: who is in the cluster, and which node is the primary
SHOW CLUSTER MEMBERS;
-- Node_id  Address         Role
-- 1        10.0.0.1:4307   PRIMARY
-- 2        10.0.0.2:4307   SECONDARY
-- 3        10.0.0.3:4307   SECONDARY

-- on the primary, as root: take node 3 out of the cluster
ALTER CLUSTER REMOVE NODE 3;

-- on the primary, as root: make node 2 the primary (before maintenance on
-- this node, say)
ALTER CLUSTER SET PRIMARY 2;
```

`ALTER CLUSTER SET PRIMARY` returns when the named node is the primary. The
role is handed over by asking that node to stand for election; if the request
reaches it late, another node may be elected instead. The statement then
fails and names the node that leads now: repeat it on that node.

A node that has lost its data directory must be removed (`ALTER CLUSTER
REMOVE NODE`) before a new node may join at its address: the cluster will not
hand the old node's place to a node that has none of what the old one had.

A removed node is told, and stops; one that was down at the time finds out
when it is started, and refuses to run. Its data directory is of no further use
to the cluster: to bring the machine back, empty the directory and start
the node with `--join` again (it gets a new node ID). Remove a failed node
that is not coming back, so that it no longer counts towards the majority
the cluster needs.

## First-Time Initialization

Database initialization is handled by the separate `roodb_init` binary. The server (`roodb`) requires an initialized database and will exit with an error if the database has not been initialized.

### roodb_init Usage

```
roodb_init --data-dir PATH
```

| Argument | Default | Description |
|----------|---------|-------------|
| data_dir | ./data | Data directory path |

**Exit codes:**
- `0` - Success (initialized or already initialized - idempotent)
- `2` - No password configured
- `3` - Storage error

### Password Configuration

Set the root password via environment variable:

| Variable | Description |
|----------|-------------|
| `ROODB_ROOT_PASSWORD` | Set root password directly |
| `ROODB_ROOT_PASSWORD_FILE` | Read password from file (Docker secrets) |

Priority: `ROODB_ROOT_PASSWORD` takes precedence over `ROODB_ROOT_PASSWORD_FILE`.

### Examples

**Bare metal:**
```bash
ROODB_ROOT_PASSWORD=mysecretpassword ./roodb_init --data-dir ./data
./roodb --port 3307 --data-dir ./data --cert-path ./certs/server.crt --key-path ./certs/server.key
```

**Container deployment:**
```bash
# Initialize (run once or as idempotent entrypoint)
docker run -e ROODB_ROOT_PASSWORD=mysecretpassword -v ./data:/data roodb roodb_init --data-dir /data

# Start server
docker run -v ./data:/data roodb roodb --port 3307 --data-dir /data --cert-path /certs/server.crt --key-path /certs/server.key
```

**Docker secrets:**
```bash
docker run -e ROODB_ROOT_PASSWORD_FILE=/run/secrets/db_password \
    -v /path/to/secrets:/run/secrets:ro \
    -v ./data:/data roodb roodb_init --data-dir /data
```

### What Gets Initialized

When `roodb_init` runs on an empty data directory:

1. Creates root user (`root@%`) with the configured password
2. Grants `ALL PRIVILEGES ON *.*` with `GRANT OPTION` to root
3. Writes schema version marker

System tables (`system.tables`, `system.columns`, etc.) are created by the catalog at server startup, not by initialization.

## User Management

> **Status:** account management through SQL does not work yet. `CREATE USER`,
> `ALTER USER`, `DROP USER`, `SET PASSWORD` and `SHOW GRANTS` are not
> functional, and an account created any other way cannot log in; `root`, set
> up by `roodb_init`, is the only usable account. This section describes the
> intended interface. [SQL.md](SQL.md#users-and-privileges) states what is
> accepted today.

### Creating Users

```sql
CREATE USER 'username'@'host' IDENTIFIED BY 'password';
```

Host patterns:
- `%` - matches any host
- `localhost` - local connections only
- `192.168.%` - IP wildcard matching
- `192.168.1.100` - specific IP

Examples:
```sql
-- User accessible from any host
CREATE USER 'app_user'@'%' IDENTIFIED BY 'app_password';

-- Local connections only
CREATE USER 'local_admin'@'localhost' IDENTIFIED BY 'admin_pass';

-- Specific subnet
CREATE USER 'internal'@'192.168.%' IDENTIFIED BY 'internal_pass';
```

### Changing Passwords

```sql
ALTER USER 'username'@'host' IDENTIFIED BY 'new_password';

-- Or use SET PASSWORD
SET PASSWORD FOR 'username'@'host' = 'new_password';
```

### Removing Users

```sql
DROP USER 'username'@'host';

-- Use IF EXISTS to avoid error when user doesn't exist
DROP USER IF EXISTS 'username'@'host';
```

## Privilege System

> **Status:** see the note under User Management. `GRANT` and `REVOKE` accept
> only `ALL`, `SELECT`, `INSERT`, `UPDATE`, `DELETE` and `CREATE`, and since
> only `root` can log in, and `root` bypasses the checks, they have no
> practical effect yet.

RooDB implements MySQL-compatible privileges at multiple levels.

### Privilege Levels

| Level | Syntax | Description |
|-------|--------|-------------|
| Global | `*.*` | All databases, all tables |
| Database | `db_name.*` | All tables in database |
| Table | `db_name.table_name` | Specific table |

### Privilege Types

| Privilege | Description |
|-----------|-------------|
| `ALL PRIVILEGES` | All available privileges |
| `SELECT` | Read data from tables |
| `INSERT` | Insert new rows |
| `UPDATE` | Modify existing rows |
| `DELETE` | Remove rows |
| `CREATE` | Create tables and indexes |
| `DROP` | Drop tables and indexes |
| `ALTER` | Alter table structure |
| `INDEX` | Create and drop indexes |
| `GRANT OPTION` | Grant privileges to others |

### Granting Privileges

```sql
-- Full admin access
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;

-- Read-only access to database
GRANT SELECT ON mydb.* TO 'reader'@'%';

-- Application access to specific table
GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.users TO 'app'@'%';

-- DDL permissions for developers
GRANT CREATE, DROP, ALTER, INDEX ON mydb.* TO 'developer'@'%';
```

### Revoking Privileges

```sql
REVOKE DELETE ON mydb.* FROM 'app'@'%';
REVOKE ALL PRIVILEGES ON *.* FROM 'user'@'%';
```

### Viewing Grants

```sql
SHOW GRANTS;                           -- Current user
SHOW GRANTS FOR 'username'@'host';     -- Specific user
```

## Security Recommendations

### TLS Certificates

1. Use CA-signed certificates in production
2. Rotate certificates before expiration
3. Store private keys with restricted permissions (600)
4. For clusters, each node must use a certificate signed by the shared CA
5. Protect the CA private key carefully; it can authorize new cluster members

### Network Security

1. **Firewall rules:** Restrict client port (3307) to application servers
2. **Raft port:** Restrict RPC port (4307) to cluster members only
3. **No plaintext:** RooDB enforces TLS; there is no way to disable it

### Principle of Least Privilege

1. Create separate users for each application
2. Grant only necessary privileges
3. Use database-level grants instead of global (`*.*`) when possible
4. Avoid granting `GRANT OPTION` unless necessary

### Password Management

1. Use strong passwords (12+ characters, mixed case, numbers, symbols)
2. Use `ROODB_ROOT_PASSWORD_FILE` with Docker secrets for container deployments
3. Rotate passwords periodically via `ALTER USER`

## Monitoring

### Health Checks

MySQL clients can use `COM_PING` for health checks. The server responds with OK if operational.

```bash
mysqladmin -h 127.0.0.1 -P 3307 -u root -p ping
```

### System Variables

Query system variables for configuration and version info:

```sql
SELECT @@version;                    -- 8.0.0-RooDB
SELECT @@max_allowed_packet;         -- 16777216
SELECT @@transaction_isolation;      -- REPEATABLE-READ
SELECT @@autocommit;                 -- 1
```

### System Tables

Query system tables for metadata (read-only, managed by SQL commands):

| Table | Contents |
|-------|----------|
| `system.tables` | Table definitions |
| `system.columns` | Column definitions |
| `system.indexes` | Index definitions |
| `system.constraints` | Table constraints |
| `system.databases` | Databases |
| `system.views` | View definitions |
| `system.procedures` | Stored procedures and functions |
| `system.triggers` | Trigger definitions |
| `system.table_statistics` | Optimizer statistics |
| `system.users` | User accounts |
| `system.grants` | Privileges |
| `system.roles` | Role definitions |
| `system.role_grants` | Role-to-user assignments |

**Lock monitoring:** `SHOW STATUS LIKE 'Innodb_row_lock_current_waits'` reports how many statements are waiting for a row lock right now; `Roodb_row_locks` and `Roodb_gap_locks` report how many locks are held. `innodb_lock_wait_timeout` (seconds, default 50) bounds a wait; set it per session, or with `SET GLOBAL` for sessions that connect afterwards.

**Note:** `SHOW STATUS` reports nothing else yet, and `SHOW PROCESSLIST` is empty.

## Backup and Recovery

### Backup Strategy

RooDB uses filesystem-level backups. The data directory contains all state:

```bash
# Stop the server: SIGTERM shuts it down in order (stop accepting, flush, close)
pkill -TERM roodb

# Copy entire data directory
cp -r ./data ./backup/data-$(date +%Y%m%d)

# Restart server on the same directory
./roodb
```

Copy the directory only while the server is stopped. A copy taken from a
running server is not a consistent backup: table files, `MANIFEST` and the log
change independently while it runs.

**Critical files** (see the data directory layout above):
- `MANIFEST` - which table files are live; must match the `*.sst` files
- `*.sst` - flushed data
- `raft/` - the log: every commit not yet flushed into a table file, plus the Raft vote and log position

### Restart and crash recovery

The server restarts on an existing data directory without `roodb_init`. After
a clean stop, a crash or `kill -9`, it replays the part of the log that had
not yet been flushed and then starts accepting connections; nothing that was
acknowledged to a client is lost (subject to the `--durability` mode for power
loss). A second server cannot open a directory that is in use: `LOCK` is held
while one is running.

### Single Node Recovery

1. Stop server (if running)
2. Remove or rename corrupted data directory
3. Restore data directory from backup
4. Start server

```bash
pkill roodb
mv ./data ./data.corrupted
cp -r ./backup/data-20240115 ./data
./roodb
```

### Cluster Failover

Raft handles node failures automatically:

**Leader failure:**
- Remaining nodes detect missing heartbeats (50ms interval)
- Election triggered after timeout (150-300ms)
- New leader elected if quorum exists
- Clients reconnect and retry to new leader

**Follower failure:**
- Cluster continues operating normally
- When follower rejoins, it catches up via Raft log replay
- No manual intervention required

**Quorum requirements:**
| Cluster Size | Quorum | Failures Tolerated |
|--------------|--------|-------------------|
| 3 nodes | 2 | 1 |
| 5 nodes | 3 | 2 |
| 7 nodes | 4 | 3 |

### Disaster Recovery

**Total cluster loss:**

1. Restore single node from most recent backup
2. Start as single-node cluster
3. Verify data integrity
4. (Optional) Rebuild multi-node cluster

```bash
# Restore from backup
cp -r ./backup/data-20240115 ./data

# Start single node
./roodb

# Verify data
mysql -h 127.0.0.1 -P 3307 -u root -p -e "SELECT COUNT(*) FROM mydb.mytable"
```

## Troubleshooting

### Connection Issues

| Error | Cause | Solution |
|-------|-------|----------|
| Connection refused | Server not running or wrong port | Check server process, verify port |
| SSL connection error | TLS required but client not using SSL | Add `--ssl-mode=REQUIRED` to mysql client |
| Access denied | Wrong username/password/host | Verify credentials and host pattern |

### Server Won't Start

| Error | Cause | Solution |
|-------|-------|----------|
| "ROODB_ROOT_PASSWORD required" | First boot without password env var | Set ROODB_ROOT_PASSWORD environment variable |
| "Failed to load certificate" | Missing or invalid TLS certificate | Generate certificate or check path |
| "Address already in use" | Port conflict | Check for other processes on port 3307 |

### Cluster Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Error 1290 on a write | The statement was sent to a secondary | Send writes to the primary: `SHOW CLUSTER MEMBERS` on any node names it |
| Error 3101 on `COMMIT` | The primary changed while the transaction was open | Run the transaction again on the current primary |
| Every node refuses writes; `SHOW CLUSTER MEMBERS` shows no `PRIMARY` | Fewer than a majority of the members are up or can reach each other, so none can be elected | Bring nodes back or restore the network. If nodes are gone for good, the cluster cannot recover by itself: restore from a backup |
| A node started with `--join` waits and then fails with "could not join the cluster" | The member it was pointed at is unreachable, or the cluster has no primary to admit it | Check the address (the member's Raft port, not its client port), the firewall, and that the cluster has a primary |
| A node joins but never leaves `LEARNER`, or the others log that it is unreachable | The others cannot reach it at the address it advertises (shown by `SHOW CLUSTER MEMBERS`) | Start it with `--advertise-host` set to a name or address the other nodes can reach |
| Nodes log TLS errors about each other's certificates | The nodes' certificates are not all signed by the CA given with `--raft-ca-cert-path` | Issue every node's certificate from the same CA |
| The primary died right after nodes joined, and no node takes over | The joiners were still `LEARNER`: a node is given a vote only once it has received the data, and until then the cluster has no more voters than before | Wait for a joiner to show as `SECONDARY` before counting on it; here, restart the old primary |
| A node refuses to start: "this node has been removed from its cluster" | It was removed with `ALTER CLUSTER REMOVE NODE` | Empty its data directory and start it with `--join` |
| A node that was away for long takes a while to accept reads again | It is being sent a copy of the data, because the log no longer reaches back far enough | Wait; `SHOW CLUSTER MEMBERS` shows it as a member throughout |

### Data Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Server refuses to start: table file or `MANIFEST` missing or damaged | Files removed or corrupted outside the server | Restore the whole data directory from backup |
| Server refuses to start: data directory in use | Another server holds `LOCK` | Stop the other server |
| Slow queries | Pending compaction | Wait for background compaction |

## Connection Examples

### MySQL CLI

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
```

### mysql_async (Rust)

```rust
let opts: Opts = OptsBuilder::default()
    .ip_or_hostname("127.0.0.1")
    .tcp_port(3307)
    .user(Some("root"))
    .pass(Some("password"))
    .ssl_opts(SslOpts::default())
    .into();
```

### JDBC

```
jdbc:mysql://127.0.0.1:3307/mydb?user=root&password=secret&useSSL=true
```

### Python (mysql-connector)

```python
import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3307,
    user="root",
    password="secret",
    ssl_disabled=False
)
```
