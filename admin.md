# RooDB Administrator's Guide

## Quick Start (Single Node)

```bash
# 1. Generate self-signed TLS certificate
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/server.key -out certs/server.crt \
    -days 365 -nodes -subj "/CN=localhost"

# 2. Initialize database (first time only)
ROODB_ROOT_PASSWORD=changeme ./roodb_init ./data

# 3. Start server
./roodb

# 4. Connect via MySQL client
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
```

## Server Configuration

### Command Line Arguments

```
./roodb [port] [data_dir] [cert_path] [key_path] [ca_cert_path]
```

| Argument | Default | Description |
|----------|---------|-------------|
| port | 3307 | Client connection port |
| data_dir | ./data | Data directory path |
| cert_path | ./certs/server.crt | TLS certificate |
| key_path | ./certs/server.key | TLS private key |
| ca_cert_path | ./certs/ca.crt | CA certificate for Raft mTLS |

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
./roodb 3307 ./data1 ./node1.crt ./node1.key ./ca.crt
./roodb 3308 ./data2 ./node2.crt ./node2.key ./ca.crt
./roodb 3309 ./data3 ./node3.crt ./node3.key ./ca.crt
```

**Testing mTLS rejection:**
A node with a certificate signed by a different CA will be rejected by the cluster. This protects against rogue nodes attempting to join.

### Port Configuration

| Port | Purpose |
|------|---------|
| 3307 (default) | Client connections (MySQL protocol over TLS) |
| 4307 (port + 1000) | Raft RPC (inter-node replication) |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ROODB_ROOT_PASSWORD` | Set root password on first boot |
| `ROODB_ROOT_PASSWORD_FILE` | Read password from file (Docker secrets) |

Priority: `ROODB_ROOT_PASSWORD` takes precedence over `ROODB_ROOT_PASSWORD_FILE`.

## Data Directory Layout

```
data/
├── sstables/          # LSM SSTable files
│   └── *.sst          # Individual SSTable files
├── manifest.json      # SSTable metadata (critical for recovery)
└── [raft state]       # Raft log entries and vote state (LSM-backed)
```

**Important:** The `manifest.json` file tracks all SSTables. Losing it while keeping SSTables results in data loss. Always backup the entire data directory as a unit.

## Deployment Modes

### Single Node

Single-node deployment is production-ready and fully supported via CLI:

```bash
ROODB_ROOT_PASSWORD=secret ./roodb 3307 ./data ./certs/server.crt ./certs/server.key
```

The server automatically bootstraps as a single-node Raft cluster, electing itself as leader. All reads and writes are processed locally.

**Use single-node when:**
- Development and testing
- Small deployments where HA is not required
- Simplicity is preferred over fault tolerance

### Multi-Node Cluster

Multi-node clusters provide high availability through Raft consensus. Currently, cluster deployment requires programmatic setup (CLI support planned).

**Cluster requirements:**
- 3 nodes minimum (tolerates 1 failure)
- 5 nodes for higher availability (tolerates 2 failures)
- All nodes need TLS certificates signed by the same CA (mTLS)
- Shared CA certificate on all nodes for peer verification
- Network connectivity between all nodes on Raft RPC port

**Bootstrap procedure (programmatic):**

```rust
// 1. Create RaftNode for each member
let mut node1 = RaftNode::new(1, addr1, tls_config.clone(), storage1, catalog1).await?;
let mut node2 = RaftNode::new(2, addr2, tls_config.clone(), storage2, catalog2).await?;
let mut node3 = RaftNode::new(3, addr3, tls_config.clone(), storage3, catalog3).await?;

// 2. Register all peers on all nodes
node1.add_peer(2, addr2);
node1.add_peer(3, addr3);
node2.add_peer(1, addr1);
node2.add_peer(3, addr3);
node3.add_peer(1, addr1);
node3.add_peer(2, addr2);

// 3. Start RPC servers on all nodes
node1.start_rpc_server().await?;
node2.start_rpc_server().await?;
node3.start_rpc_server().await?;

// 4. Bootstrap from ONE node with full membership
let members = vec![(1, addr1), (2, addr2), (3, addr3)];
node1.bootstrap_cluster(members).await?;

// 5. Wait for leader election (~500-1000ms)
tokio::time::sleep(Duration::from_millis(1000)).await;
```

See `tests/raft_cluster.rs` for complete working example.

**Cluster behavior:**
- Writes must go to leader (followers return "not leader" error)
- Reads served from any node (local LSM)
- Leader election: 150-300ms timeout, 50ms heartbeat
- Automatic failover when leader fails (if quorum remains)

## First-Time Initialization

Database initialization is handled by the separate `roodb_init` binary. The server (`roodb`) requires an initialized database and will exit with an error if the database has not been initialized.

### roodb_init Usage

```
roodb_init [data_dir]
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
ROODB_ROOT_PASSWORD=mysecretpassword ./roodb_init ./data
./roodb 3307 ./data ./certs/server.crt ./certs/server.key
```

**Container deployment:**
```bash
# Initialize (run once or as idempotent entrypoint)
docker run -e ROODB_ROOT_PASSWORD=mysecretpassword -v ./data:/data roodb roodb_init /data

# Start server
docker run -v ./data:/data roodb roodb 3307 /data /certs/server.crt /certs/server.key
```

**Docker secrets:**
```bash
docker run -e ROODB_ROOT_PASSWORD_FILE=/run/secrets/db_password \
    -v /path/to/secrets:/run/secrets:ro \
    -v ./data:/data roodb roodb_init /data
```

### What Gets Initialized

When `roodb_init` runs on an empty data directory:

1. Creates root user (`root@%`) with the configured password
2. Grants `ALL PRIVILEGES ON *.*` with `GRANT OPTION` to root
3. Writes schema version marker

System tables (`system.tables`, `system.columns`, etc.) are created by the catalog at server startup, not by initialization.

## User Management

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
| `system.users` | User accounts |
| `system.grants` | Privileges |
| `system.roles` | Role definitions |
| `system.role_grants` | Role-to-user assignments |

**Note:** SHOW STATUS and SHOW PROCESSLIST are not yet implemented.

## Backup and Recovery

### Backup Strategy

RooDB uses filesystem-level backups. The data directory contains all state:

```bash
# Stop server for consistent backup
pkill roodb

# Copy entire data directory
cp -r ./data ./backup/data-$(date +%Y%m%d)

# Restart server
./roodb
```

For minimal downtime, you can snapshot while running (point-in-time consistency via LSM design), but stopping ensures full consistency.

**Critical files:**
- `manifest.json` - SSTable registry; must match SSTable files
- `sstables/*.sst` - Actual data
- Raft state - Cluster membership and log position

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
ROODB_ROOT_PASSWORD=secret ./roodb

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

| Error | Cause | Solution |
|-------|-------|----------|
| "Not leader" | Write sent to follower | Redirect write to leader node |
| Election not completing | Network partition or <quorum nodes | Check network connectivity, verify quorum |
| Node won't rejoin | Stale state | Clear data directory, rejoin as new node |

### Data Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Missing tables after restart | Corrupted manifest.json | Restore from backup |
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
