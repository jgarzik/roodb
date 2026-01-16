# RooDB Administrator's Guide

## First-Time Setup

### Environment Variables

RooDB supports MySQL Docker-style environment variables for container and cloud-init deployments:

| Variable | Description |
|----------|-------------|
| `ROODB_ROOT_PASSWORD` | Set root password directly |
| `ROODB_ROOT_PASSWORD_FILE` | Read password from file (Docker secrets) |

Priority: `ROODB_ROOT_PASSWORD` > `ROODB_ROOT_PASSWORD_FILE`

### Container Deployment

```bash
# Set password directly
docker run -e ROODB_ROOT_PASSWORD=mysecretpassword roodb

# Using Docker secrets
docker run -e ROODB_ROOT_PASSWORD_FILE=/run/secrets/db_password \
  -v /path/to/secrets:/run/secrets:ro roodb
```

### Manual Initialization

If no environment variables are set and the database is not initialized, the server will fail to start with an error message indicating which variable to set.

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
-- Create user accessible from any host
CREATE USER 'app_user'@'%' IDENTIFIED BY 'app_password';

-- Create user for local connections only
CREATE USER 'local_admin'@'localhost' IDENTIFIED BY 'admin_pass';

-- Create user for specific subnet
CREATE USER 'internal'@'192.168.%' IDENTIFIED BY 'internal_pass';
```

### Changing Passwords

```sql
ALTER USER 'username'@'host' IDENTIFIED BY 'new_password';
```

### Removing Users

```sql
DROP USER 'username'@'host';
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
-- Grant all privileges on everything
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;

-- Grant read-only access to specific database
GRANT SELECT ON mydb.* TO 'reader'@'%';

-- Grant read-write access to specific table
GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.users TO 'app'@'%';

-- Grant DDL permissions
GRANT CREATE, DROP, ALTER, INDEX ON mydb.* TO 'developer'@'%';
```

### Revoking Privileges

```sql
-- Revoke specific privilege
REVOKE DELETE ON mydb.* FROM 'app'@'%';

-- Revoke all privileges
REVOKE ALL PRIVILEGES ON *.* FROM 'user'@'%';
```

### Viewing Grants

```sql
-- Show grants for current user
SHOW GRANTS;

-- Show grants for specific user
SHOW GRANTS FOR 'username'@'host';
```

## Security Recommendations

### Password Policies

1. Use strong passwords (12+ characters, mixed case, numbers, symbols)
2. Rotate passwords periodically
3. Use `ROODB_ROOT_PASSWORD_FILE` with Docker secrets for container deployments

### Network Security

1. RooDB requires TLS for all connections (no plaintext option)
2. Use firewall rules to restrict access to trusted networks
3. Consider using host patterns to limit which hosts can connect

### Principle of Least Privilege

1. Create separate users for each application
2. Grant only necessary privileges
3. Use database-level grants instead of global (`*.*`) when possible
4. Avoid granting `GRANT OPTION` unless necessary

## System Tables

Auth data is stored in these system tables (read-only, managed by SQL commands):

| Table | Description |
|-------|-------------|
| `system.users` | User accounts and password hashes |
| `system.grants` | Privilege grants |
| `system.roles` | Role definitions (future) |
| `system.role_grants` | Role memberships (future) |

## Troubleshooting

### "Access denied" errors

1. Verify username and password are correct
2. Check that user exists: query `system.users` if you have access
3. Verify host pattern matches client's address

### "Account is locked" error

The user's account has been locked. An administrator must unlock it.

### "Password has expired" error

The user's password has expired. Change it using `ALTER USER`.

## Connection String Examples

### MySQL CLI

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p
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
