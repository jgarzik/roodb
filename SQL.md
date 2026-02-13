# RooDB SQL Reference

## Data Types

| Type | Description |
|------|-------------|
| `BOOLEAN` | true/false |
| `TINYINT` | 8-bit signed integer |
| `SMALLINT` | 16-bit signed integer |
| `INT` | 32-bit signed integer |
| `BIGINT` | 64-bit signed integer |
| `FLOAT` | 32-bit floating point |
| `DOUBLE` | 64-bit floating point |
| `VARCHAR(n)` | Variable-length string, max n chars |
| `TEXT` | Unlimited-length string |
| `BLOB` | Binary data |
| `TIMESTAMP` | Date and time |

**Type aliases** (accepted in DDL, stored as the canonical type):

| Alias | Stored As |
|-------|-----------|
| `INTEGER` | `INT` |
| `REAL` | `FLOAT` |
| `DOUBLE PRECISION` | `DOUBLE` |
| `CHAR(n)` | `VARCHAR(n)` |
| `DATETIME` | `TIMESTAMP` |
| `BINARY(n)`, `VARBINARY(n)` | `BLOB` |

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

**Constraint enforcement:**

| Constraint | Enforced? |
|------------|-----------|
| `PRIMARY KEY` | Yes |
| `UNIQUE` | Yes |
| `NOT NULL` | Yes |
| `FOREIGN KEY` | No — parsed and stored in `system.constraints`, but never checked at runtime |
| `CHECK` | No — parsed and stored in `system.constraints`, but never checked at runtime |

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

**Join algorithms:** HashJoin for equi-joins (`a.col = b.col`), NestedLoopJoin for all others.

**Expression-only queries:** `SELECT 1+1`, `SELECT UPPER('foo')` work without a FROM clause (internally uses a single empty row).

### INSERT

```
INSERT INTO table [(column, ...)] VALUES (expr, ...), ...
```

Multi-row VALUES supported. No INSERT...SELECT.

### UPDATE

```
UPDATE table SET column = expr, ... [WHERE condition]
```

Single-table only.

### DELETE

```
DELETE FROM table [WHERE condition]
```

Single-table only.

## Expressions & Operators

### Binary Operators

| Category | Operators |
|----------|-----------|
| Arithmetic | `+` `-` `*` `/` `%` |
| Comparison | `=` `!=` `<>` `<` `<=` `>` `>=` |
| Logical | `AND` `OR` |
| Pattern | `LIKE` `NOT LIKE` |

### Unary Operators

| Operator | Description |
|----------|-------------|
| `NOT` | Logical negation |
| `-` | Arithmetic negation |

### Other Expressions

| Expression | Description |
|------------|-------------|
| `IS NULL` / `IS NOT NULL` | Null test |
| `IN (value, ...)` | Membership (value list only, no subquery) |
| `NOT IN (value, ...)` | Negated membership |
| `BETWEEN x AND y` | Range (inclusive) |
| `NOT BETWEEN x AND y` | Negated range |

**LIKE patterns:** `%` = any chars, `_` = single char

### NULL Semantics

- **Arithmetic/comparison with NULL:** result is NULL (standard SQL propagation)
- **AND:** `FALSE AND NULL` = `FALSE`; `TRUE AND NULL` = `NULL`
- **OR:** `TRUE OR NULL` = `TRUE`; `FALSE OR NULL` = `NULL`
- **IN:** NULL in the expression → NULL result; NULL items in the list are skipped
- **BETWEEN:** any NULL operand → NULL result
- **IS NULL / IS NOT NULL:** always returns TRUE or FALSE, never NULL
- **Join keys:** NULL != NULL (NULL keys never match in joins)

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

### Scalar (implemented at runtime)

| Function | Description |
|----------|-------------|
| `UPPER(s)`, `UCASE(s)` | Uppercase |
| `LOWER(s)`, `LCASE(s)` | Lowercase |
| `LENGTH(s)`, `LEN(s)` | Byte length of string or blob |
| `CONCAT(a, b, ...)` | Concatenate (returns NULL if any arg is NULL) |
| `COALESCE(a, b, ...)` | First non-NULL value |
| `NULLIF(a, b)` | NULL if a = b, else a |
| `ABS(n)` | Absolute value |

**Not implemented:** The resolver accepts `TRIM`, `LTRIM`, `RTRIM`, `SUBSTRING`/`SUBSTR`, `CHAR_LENGTH`, `IFNULL`, `CEIL`, `FLOOR`, `ROUND`, `NOW`, `CURRENT_TIMESTAMP` for type inference, but they are **not implemented** in the executor and will produce a runtime error ("unknown function").

### Aggregate

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Row count |
| `COUNT([DISTINCT] expr)` | Non-NULL count |
| `SUM([DISTINCT] expr)` | Sum |
| `AVG([DISTINCT] expr)` | Average |
| `MIN(expr)` | Minimum |
| `MAX(expr)` | Maximum |

All aggregate functions support `DISTINCT`.

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
| `SERIALIZABLE` | No | Returns error (not implemented) |

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

## System Variables

`SELECT @@variable` returns compatibility values:

| Variable | Value |
|----------|-------|
| `version` | `8.0.0-RooDB` |
| `version_comment` | `RooDB` |
| `max_allowed_packet` | `16777216` |
| `character_set_client` | `utf8mb4` |
| `character_set_connection` | `utf8mb4` |
| `character_set_results` | `utf8mb4` |
| `collation_connection` | `utf8mb4_general_ci` |
| `sql_mode` | `STRICT_TRANS_TABLES` |
| `transaction_isolation` | `REPEATABLE-READ` |
| `tx_isolation` | `REPEATABLE-READ` |
| `autocommit` | `1` |
| `time_zone` | `SYSTEM` |
| `system_time_zone` | `UTC` |
| `wait_timeout` | `28800` |
| `interactive_timeout` | `28800` |
| `net_write_timeout` | `60` |
| `net_read_timeout` | `30` |
| `socket` | `/tmp/roodb.sock` |

Unknown variables return an empty string.

## System Tables

The following `system.*` tables are queryable via `SELECT`:

| Table | Description |
|-------|-------------|
| `system.tables` | Table names |
| `system.columns` | Column definitions (name, type, nullable, default, auto_increment) |
| `system.indexes` | Index definitions |
| `system.constraints` | Constraint definitions (PK, UNIQUE, FK, CHECK) |
| `system.users` | User accounts |
| `system.grants` | Privilege grants |
| `system.roles` | Role definitions |
| `system.role_grants` | Role-to-user mappings |

## SET Statements

| Statement | Behavior |
|-----------|----------|
| `SET AUTOCOMMIT = ...` | Functional — controls transaction auto-commit |
| `SET [SESSION] TRANSACTION ISOLATION LEVEL ...` | Functional — changes isolation level |
| `SET NAMES ...` | Silently accepted (no-op) |
| `SET CHARACTER SET ...` | Silently accepted (no-op) |
| Any other `SET ...` | Silently accepted (no-op) |

## Prepared Statements

Protocol-level prepared statements are supported:

- `COM_STMT_PREPARE` — parse SQL, cache plan, return statement ID
- `COM_STMT_EXECUTE` — bind `?` placeholder parameters and execute
- `COM_STMT_CLOSE` — deallocate a prepared statement
- `COM_STMT_RESET` — reset cursor state (currently a no-op)

Plans are cached per-statement and invalidated when the schema version changes.

## Query Optimizations

| Optimization | When Applied |
|--------------|-------------|
| **PointGet** | Single-column PK equality in WHERE (`WHERE pk = value`) — O(log n) |
| **RangeScan** | PK range in WHERE (`WHERE pk BETWEEN a AND b`, `WHERE pk > a AND pk < b`) |
| **HashJoin** | Equi-join conditions (`a.col = b.col`) — O(n+m) |
| **NestedLoopJoin** | Non-equi joins, cross joins — O(n*m) |

## Limitations

The following SQL features are **not supported**:

- `CASE` / `WHEN` expressions
- `CAST` / type conversion expressions
- Subqueries (`IN (SELECT ...)`, `EXISTS`, scalar subqueries, correlated subqueries)
- Derived tables (`FROM (SELECT ...)`)
- Common table expressions (`WITH ... AS`)
- `UNION` / `INTERSECT` / `EXCEPT`
- Window functions (`OVER`, `PARTITION BY`, `ROW_NUMBER`, etc.)
- `ALTER TABLE`
- `TRUNCATE TABLE`
- `INSERT ... SELECT`
- `REPLACE` / `ON DUPLICATE KEY UPDATE`
- Multi-table `UPDATE` / `DELETE`
- `SAVEPOINT` / `RELEASE SAVEPOINT`
- Stored procedures, triggers, views
- `SHOW TABLES` / `SHOW COLUMNS` / `SHOW DATABASES` / `DESCRIBE`
- `EXPLAIN` (formatter exists but no SQL dispatch path)
