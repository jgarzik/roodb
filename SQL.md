# RooDB SQL Reference

## Data Types

| Type | Description |
|------|-------------|
| `BOOLEAN` | true/false |
| `TINYINT` | 8-bit signed integer |
| `SMALLINT` | 16-bit signed integer |
| `INT`, `INTEGER` | 32-bit signed integer |
| `BIGINT` | 64-bit signed integer |
| `FLOAT` | 32-bit floating point |
| `DOUBLE`, `DOUBLE PRECISION` | 64-bit floating point |
| `VARCHAR(n)` | Variable-length string, max n chars |
| `CHAR(n)` | Fixed-length string (stored as VARCHAR) |
| `TEXT` | Unlimited-length string |
| `BLOB` | Binary data |
| `BINARY(n)`, `VARBINARY(n)` | Binary data (stored as BLOB) |
| `TIMESTAMP`, `DATETIME` | Date and time |

## DDL

### CREATE TABLE

```
CREATE TABLE [IF NOT EXISTS] name (
    column_name type [NOT NULL] [DEFAULT value] [AUTO_INCREMENT],
    ...
    [PRIMARY KEY (columns)]
    [UNIQUE (columns)]
    [FOREIGN KEY (columns) REFERENCES table (columns)]
    [CHECK (expression)]
)
```

### DROP TABLE

```
DROP TABLE [IF EXISTS] name
```

### CREATE INDEX

```
CREATE [UNIQUE] INDEX name ON table (column, ...)
```

### DROP INDEX

```
DROP INDEX name
```

## DML

### SELECT

```
SELECT [DISTINCT] select_list
FROM table [[AS] alias], ...
[join_clause ...]
[WHERE condition]
[GROUP BY expr, ...]
[HAVING condition]
[ORDER BY expr [ASC|DESC], ...]
[LIMIT count [OFFSET skip]]
```

**select_list:** `*` | `table.*` | `expr [[AS] alias]`, ...

**join_clause:**
- `[INNER] JOIN table ON condition`
- `LEFT [OUTER] JOIN table ON condition`
- `RIGHT [OUTER] JOIN table ON condition`
- `FULL [OUTER] JOIN table ON condition`
- `CROSS JOIN table`

### INSERT

```
INSERT INTO table [(column, ...)] VALUES (expr, ...), ...
```

### UPDATE

```
UPDATE table SET column = expr, ... [WHERE condition]
```

### DELETE

```
DELETE FROM table [WHERE condition]
```

## Transaction Control

| Statement | Description |
|-----------|-------------|
| `BEGIN` | Start transaction |
| `START TRANSACTION` | Start transaction |
| `COMMIT` | Commit transaction |
| `ROLLBACK` | Abort transaction |
| `SET AUTOCOMMIT = {0\|1\|ON\|OFF}` | Toggle autocommit |
| `SET [SESSION] TRANSACTION ISOLATION LEVEL level` | Set isolation |

**Isolation levels:** `READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`

### Isolation Level Details

RooDB uses **InnoDB-style snapshot isolation** via MVCC. Default: `REPEATABLE READ`.

| Level | Support | Behavior |
|-------|---------|----------|
| `READ UNCOMMITTED` | Partial | Falls back to READ COMMITTED |
| `READ COMMITTED` | Yes | Each statement sees latest committed data |
| `REPEATABLE READ` | Yes | Snapshot at first read, consistent for transaction |
| `SERIALIZABLE` | Partial | Falls back to REPEATABLE READ (no gap locks) |

**Anomaly prevention at REPEATABLE READ:**

| Anomaly | Prevented? | Mechanism |
|---------|------------|-----------|
| Dirty reads | Yes | MVCC visibility |
| Non-repeatable reads | Yes | Snapshot isolation |
| Lost updates | Yes | OCC version check |
| Phantom reads | Mostly | OCC (not gap locks) |

**Write conflicts:** Concurrent UPDATE/DELETE on same row detected at commit (Raft apply). Client receives error:
```
Write conflict: row in 'table' was modified by another transaction
```

## User Management

### CREATE USER

```
CREATE USER [IF NOT EXISTS] 'name'@'host' [IDENTIFIED BY 'password']
```

### ALTER USER

```
ALTER USER 'name'@'host' IDENTIFIED BY 'password'
```

### DROP USER

```
DROP USER [IF EXISTS] 'name'@'host'
```

### SET PASSWORD

```
SET PASSWORD FOR 'name'@'host' = 'password'
```

### GRANT

```
GRANT privilege, ... ON object TO 'user'@'host' [WITH GRANT OPTION]
```

**Privileges:** `ALL`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `INDEX`, `GRANT OPTION`

**Objects:** `*.*` | `database.*` | `database.table`

### REVOKE

```
REVOKE privilege, ... ON object FROM 'user'@'host'
```

### SHOW GRANTS

```
SHOW GRANTS [FOR 'user'@'host']
```

## Operators

| Category | Operators |
|----------|-----------|
| Arithmetic | `+` `-` `*` `/` `%` |
| Comparison | `=` `!=` `<>` `<` `<=` `>` `>=` |
| Logical | `AND` `OR` `NOT` |
| Pattern | `LIKE` `NOT LIKE` |
| Null test | `IS NULL` `IS NOT NULL` |
| Membership | `IN (list)` `NOT IN (list)` |
| Range | `BETWEEN x AND y` `NOT BETWEEN x AND y` |

**LIKE patterns:** `%` = any chars, `_` = single char

## Literals

| Type | Syntax |
|------|--------|
| Null | `NULL` |
| Boolean | `TRUE` `FALSE` |
| Integer | `123` `-456` |
| Float | `1.5` `-3.14` `1e10` |
| String | `'text'` |
| Blob | `X'deadbeef'` |

## Functions

### Scalar

| Function | Description |
|----------|-------------|
| `UPPER(s)`, `UCASE(s)` | Uppercase |
| `LOWER(s)`, `LCASE(s)` | Lowercase |
| `LENGTH(s)`, `LEN(s)` | String/bytes length |
| `CONCAT(a, b, ...)` | Concatenate |
| `COALESCE(a, b, ...)` | First non-NULL |
| `NULLIF(a, b)` | NULL if a=b, else a |
| `ABS(n)` | Absolute value |

### Aggregate

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Row count |
| `COUNT([DISTINCT] col)` | Non-NULL count |
| `SUM([DISTINCT] col)` | Sum |
| `AVG([DISTINCT] col)` | Average |
| `MIN(col)` | Minimum |
| `MAX(col)` | Maximum |

## System Variables

`SELECT @@variable` returns compatibility values:

| Variable | Value |
|----------|-------|
| `version` | `8.0.0-RooDB` |
| `max_allowed_packet` | `16777216` |
| `character_set_client` | `utf8mb4` |
| `character_set_connection` | `utf8mb4` |
| `character_set_results` | `utf8mb4` |
| `collation_connection` | `utf8mb4_general_ci` |
| `sql_mode` | `STRICT_TRANS_TABLES` |
| `transaction_isolation` | `REPEATABLE-READ` |
| `autocommit` | `1` |
| `time_zone` | `SYSTEM` |
| `wait_timeout` | `28800` |
