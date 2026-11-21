# RooDB SQL Reference

RooDB speaks the MySQL dialect of SQL. This document describes what the
server accepts today and how it behaves, including where that differs from
MySQL. Operational topics (startup flags, durability, restart, the SQL
Server protocol endpoint) are in [admin.md](admin.md).

Statements marked *accepted, no effect* are parsed and acknowledged but do
nothing. The section [Limitations and differences from MySQL](#limitations-and-differences-from-mysql)
collects the behaviour most likely to surprise.

## Statements and names

- One statement per request. A request containing several statements
  separated by `;` is refused (`Multiple statements not supported`). The
  TDS endpoint splits batches itself, see [TDS driver shims](#tds-driver-shims).
- Comments: `-- text`, `# text`, `/* text */`. MySQL conditional comments
  `/*!NNNNN code */` are executed when `NNNNN` is at most `80045`, and
  `/*! code */` always.
- Table names are case-sensitive. Column names are matched without regard
  to case. Function names and keywords are case-insensitive.
- Column names may be quoted with backticks. **Table names may not**: the
  backticks become part of the name (`CREATE TABLE \`t\`` creates a table
  that `SELECT * FROM t` does not find).
- There is one table namespace for the whole server. A table name cannot be
  qualified with a database name (`db.t` is taken as a table literally named
  `db.t`); the only qualified names are the `system.*` tables. See
  [Databases](#databases).
- Double-quoted text is a string literal, as in MySQL's default mode.

## Data Types

Every declared type is stored as one of the following.

| Stored type | Declared as | Notes |
|-------------|-------------|-------|
| `BOOLEAN` | `BOOLEAN`, `BOOL` | Returned as `1` / `0` |
| `TINYINT` | `TINYINT`, `TINYINT UNSIGNED` | |
| `SMALLINT` | `SMALLINT`, `INT2`, `YEAR`, and their `UNSIGNED` forms | |
| `INT` | `INT`, `INTEGER`, `MEDIUMINT`, `INT4`, and their `UNSIGNED` forms | |
| `BIGINT` | `BIGINT`, `INT8`, `SERIAL` | 64-bit signed |
| `BIGINT UNSIGNED` | `BIGINT UNSIGNED` | 64-bit unsigned |
| `FLOAT` | `FLOAT`, `FLOAT(p)` with p ≤ 24, `FLOAT(M,D)`, `REAL`, `FLOAT4` | |
| `DOUBLE` | `DOUBLE`, `DOUBLE(M,D)`, `DOUBLE PRECISION`, `FLOAT8`, `FLOAT(p)` with 25 ≤ p ≤ 53, `FIXED` | |
| `DECIMAL(p,s)` | `DECIMAL`, `NUMERIC`, `DEC` (and `UNSIGNED` forms) | Default `(10,0)`; p capped at 65, s at 30 |
| `VARCHAR(n)` | `VARCHAR(n)`, `CHAR(n)`, `CHARACTER VARYING(n)`, `NVARCHAR(n)` | `CHAR` defaults to 1 and may not exceed 255; `VARCHAR` without a length is 255; `VARCHAR(n)` with n > 65535 becomes `TEXT` |
| `TEXT` | `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`, `TEXT(n)`, `LONG`, `CLOB`, `NCHAR`, `ENUM(...)`, `SET(...)`, `TIME` | `ENUM`, `SET` and `TIME` values are kept as plain text; the value list is not checked |
| `BLOB` | `BLOB`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`, `BLOB(n)`, `BINARY(n)`, `VARBINARY(n)`, `LONG BYTE` | |
| `BIT(m)` | `BIT`, `BIT(m)` | m = 1..64 (default 1; larger widths become 64); returned as bytes |
| `TIMESTAMP` | `TIMESTAMP`, `DATETIME`, `DATE` | One date-and-time type; fractional seconds are kept |
| `JSON` | `JSON` | |
| `GEOMETRY` | `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | Stored as WKB |

`DESCRIBE` and `SHOW CREATE TABLE` show the stored type, not the type as
declared. `ZEROFILL`, display widths, `CHARACTER SET` and `COLLATE` on a
column are accepted and ignored.

**Declared sizes are not enforced.** All signed integer types hold any
64-bit value (`TINYINT` accepts `128`), unsigned types accept negative
input, `VARCHAR(n)` and `CHAR(n)` accept longer strings, and `DECIMAL(p,s)`
values are stored with the precision and scale they were written with, not
rounded to the column's. A string that is not a valid date is stored in a
`TIMESTAMP` column as given. This holds whatever `sql_mode` is set.

Other type names (for example `UUID`, array types) are refused.

## DDL

### Databases

```
CREATE DATABASE [IF NOT EXISTS] name
DROP {DATABASE | SCHEMA} [IF EXISTS] name
USE name
SHOW DATABASES
```

A new server has the databases `default` and `test`; a session starts in
`default` unless the client selects another. Databases are names only: they
can be created, listed, selected with `USE` and reported by `DATABASE()`,
but **all tables live in one shared namespace** and are visible whichever
database is selected. `SHOW TABLES FROM db` lists every table. `default`
cannot be dropped. `CREATE SCHEMA` is not accepted (`DROP SCHEMA` is).
`CHARACTER SET` and similar options on `CREATE DATABASE` are ignored.

### CREATE TABLE

```
CREATE TABLE [IF NOT EXISTS] name (
    column_name type [NOT NULL | NULL] [DEFAULT value] [AUTO_INCREMENT] [PRIMARY KEY],
    ...
    [PRIMARY KEY (column, ...)]
    [UNIQUE (column, ...)]
    [[CONSTRAINT name] FOREIGN KEY (column, ...) REFERENCES table (column, ...)]
    [[CONSTRAINT name] CHECK (expression)]
)

CREATE TABLE name [(column definitions)] AS SELECT ...
```

Table options (`ENGINE=`, `DEFAULT CHARSET=`, `COLLATE=`, `COMMENT=`,
`AUTO_INCREMENT=`) and column `COMMENT` are accepted and ignored.
`CREATE TABLE ... AS SELECT` creates the table from the query's columns and
copies the rows.

**Constraints and column options:**

| Item | Behaviour |
|------|-----------|
| `PRIMARY KEY` | Enforced. One or more columns; the columns become `NOT NULL`. A duplicate key is error 1062. Rows are stored and looked up by primary key. |
| No primary key | Allowed. Rows get a hidden internal identifier; duplicates are permitted. |
| `NOT NULL` | Enforced for a single-row `INSERT` (error 1048). In a multi-row `INSERT`, in `INSERT IGNORE` and in `UPDATE`, a `NULL` is replaced by the column default or the type's zero value (`0`, `''`) instead of being refused. |
| `DEFAULT value` | Literal defaults are applied when a column is omitted and for the `DEFAULT` keyword in `VALUES`. `DEFAULT CURRENT_TIMESTAMP` is accepted but does not work: an `INSERT` that omits the column fails with a type error. |
| `AUTO_INCREMENT` | When the column is omitted, `NULL` or `0`, a unique increasing number is generated and reported by `LAST_INSERT_ID()`. Numbers come from a server-wide counter: they do not start at 1, are not consecutive, and are not reset by `TRUNCATE`. |
| `UNIQUE` (table-level) | Recorded in `system.constraints`, **not enforced**. A column-level `UNIQUE` is ignored entirely. |
| `FOREIGN KEY` (table-level) | Recorded in `system.constraints`, **not enforced**: no check on insert, update, delete or `DROP TABLE` of the referenced table. Naming a referenced table that does not exist leaves a table that cannot be used. A column-level `REFERENCES` clause is ignored. |
| `CHECK` (table-level) | Recorded in `system.constraints`, **not enforced**. A column-level `CHECK` is ignored. |
| `KEY` / `INDEX` / `UNIQUE KEY` inside `CREATE TABLE` | Accepted and ignored (no index entry is recorded). |
| Generated columns (`GENERATED ALWAYS AS`) | Accepted; the expression is not evaluated (the column reads `NULL`). |
| `ON UPDATE CURRENT_TIMESTAMP` | Accepted and ignored. |

`CREATE TABLE ... LIKE` is not supported (it reports success but the table
is unusable).

### ALTER TABLE

```
ALTER TABLE name operation [, operation ...]
```

| Operation | |
|-----------|---|
| `ADD [COLUMN] column_definition` | Appended as the last column; `FIRST` / `AFTER` are ignored |
| `DROP [COLUMN] name` | Also removes constraints naming the column |
| `MODIFY [COLUMN] name type [options]` | |
| `CHANGE [COLUMN] old new type [options]` | |
| `RENAME COLUMN old TO new` | |
| `ALTER [COLUMN] name SET DEFAULT value` / `DROP DEFAULT` | |
| `ADD [CONSTRAINT name] {PRIMARY KEY \| UNIQUE \| FOREIGN KEY \| CHECK} ...`, `ADD {INDEX \| KEY} ...` | Recorded only, as in `CREATE TABLE` |
| `DROP PRIMARY KEY`, `DROP FOREIGN KEY name`, `DROP INDEX name`, `DROP CONSTRAINT name` | |
| `RENAME TO new_name` | |
| `ALGORITHM=...`, `LOCK=...` | Accepted, no effect |

**`ALTER TABLE` changes the table definition only; stored rows are not
rewritten.** On a table that already has rows:

- after `ADD COLUMN`, existing rows read the new column as `0` / empty, not
  as its `DEFAULT`;
- after `DROP COLUMN` of any column but the last, existing rows are read
  with their values shifted into the wrong columns;
- after `RENAME TO`, the rows stay behind under the old name: the renamed
  table reads as empty;
- adding, dropping or changing the primary key does not re-key existing rows.

Use these forms on empty tables. Other forms (`ENGINE=`, `COMMENT=`,
`CONVERT TO`, `ORDER BY`, partitioning) are refused.

`RENAME TABLE a TO b` is not supported.

### DROP TABLE, TRUNCATE

```
DROP TABLE [IF EXISTS] name [, name ...]
TRUNCATE [TABLE] name
```

`DROP TABLE` removes the table, its rows, its index entries and its
triggers. `TRUNCATE` deletes all rows (it is executed as an unfiltered
`DELETE`).

### Indexes

```
CREATE [UNIQUE] INDEX name ON table (column, ...)
DROP INDEX name [ON table]
```

Secondary indexes are **metadata only**. `CREATE INDEX` records the index in
`system.indexes` (shown by `SHOW INDEX`); it does not build an index
structure, is not used by queries, and `UNIQUE` is not enforced. Only the
primary key speeds up lookups. Index names are global. Prefix lengths
(`col(10)`) and `FULLTEXT` / `SPATIAL` indexes are refused. Creating an
index whose name exists, and dropping one that does not, succeed silently.

### Views

```
CREATE [OR REPLACE] VIEW name AS select_statement
DROP VIEW [IF EXISTS] name [, name ...]
```

A view stores its query text (`system.views`) and is expanded when used in
`FROM`; it survives restarts. Views can be queried, joined, and defined on
other views. They are read-only: `INSERT`, `UPDATE` and `DELETE` on a view
fail. The query is only checked for syntax at creation, so a view on a
missing table is created and fails when used. A view cannot be defined on a
temporary table (error 1352). A column list
(`CREATE VIEW v (a, b) AS ...`) is ignored. `SHOW CREATE TABLE view` shows
the definition; `DESCRIBE view` and `SHOW CREATE VIEW` do not work. Views
appear in `SHOW TABLES`.

### Temporary tables

```
CREATE TEMPORARY TABLE [IF NOT EXISTS] name (...)
DROP [TEMPORARY] TABLE name
```

- Rows live only in the memory of the session that created the table. They
  are never written to disk or replicated, and are gone when the session
  disconnects (or resets its connection).
- A session may hold at most 256 MiB of temporary-table rows.
- To every other session the table does not exist: any statement naming
  it, in any position (a join, a subquery, a `UNION` branch,
  `INSERT ... SELECT`, `CREATE INDEX`, `ALTER TABLE`, `CREATE TRIGGER`,
  `DROP TABLE`), fails with error 1146 (`Table 'name' doesn't exist`).
- The name is nevertheless reserved server-wide while the table exists: a
  temporary table cannot have the name of an existing table (it does not
  shadow it), and no other session can create a table, temporary or not,
  with that name.
- Temporary tables take part in transactions like other tables (`ROLLBACK`
  discards uncommitted changes to them), and the primary key is enforced.
- Triggers and views cannot be created on temporary tables.
- `ALTER TABLE` on a temporary table changes it for its session only
  (renaming one is not supported). `DROP TABLE IF EXISTS` naming another
  session's temporary table succeeds and does nothing.
- Dropping a temporary table inside a transaction discards the
  transaction's uncommitted changes to it.
- `CREATE TEMPORARY TABLE ... AS SELECT` creates the columns but copies no rows.

### Triggers

```
CREATE TRIGGER name {BEFORE | AFTER} {INSERT | UPDATE | DELETE} ON table
    FOR EACH ROW statement
DROP TRIGGER [IF EXISTS] name
```

Triggers are stored in `system.triggers` and survive restarts. Trigger names
are global; creating a second trigger with an existing name is refused.
The table must exist, and cannot be a `system.*` table or a temporary table.

What actually runs:

| Trigger | Fires? |
|---------|--------|
| `BEFORE INSERT`, `AFTER INSERT` | Yes, for `INSERT ... VALUES` (including `ON DUPLICATE KEY UPDATE`, and inserts through prepared statements). Both run **after** the row has been inserted, `BEFORE` triggers first. |
| `INSERT` triggers, for rows added by `INSERT ... SELECT`, `REPLACE` or `LOAD DATA` | **No.** |
| `BEFORE/AFTER UPDATE`, `BEFORE/AFTER DELETE` | **No.** They are stored but never fire. |

The body is a single statement (no `BEGIN ... END` block). `NEW.column` is
replaced by the inserted value; `OLD` is not available. A trigger cannot
change the row (`SET NEW.col = ...` makes the insert fail). If the trigger
statement fails, the `INSERT` fails. Inside a transaction the trigger's
changes commit or roll back with it. There is no `SHOW TRIGGERS` or
`SHOW CREATE TRIGGER`; query `system.triggers`.

### Stored procedures

```
CREATE PROCEDURE name ([{IN | OUT | INOUT}] param type, ...) BEGIN statements END
CREATE PROCEDURE name (...) single_statement      -- SELECT, INSERT, UPDATE, DELETE or SET
DROP PROCEDURE [IF EXISTS] name
CALL name(arguments)
SHOW PROCEDURE STATUS [LIKE 'pattern']
SHOW CREATE PROCEDURE name
```

Procedures are stored in `system.procedures`, are visible to every session
and survive restarts. Procedures and functions share one namespace. `OUT`
and `INOUT` arguments must be user variables (`@v`) to receive a value.

Statements supported inside a body:

| Statement | Notes |
|-----------|-------|
| `DECLARE var type [DEFAULT expr]` (several names allowed) | |
| `SET var = expr`, `SET @uservar = expr` | |
| `IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF` | |
| `CASE ... WHEN ... THEN ... [ELSE ...] END CASE` | Both forms |
| `WHILE cond DO ... END WHILE` | Stops with an error after 100,000 iterations |
| `SELECT ... INTO var [, var ...]` | First row only |
| `SELECT ...` / `SHOW ...` | The result set is sent to the client |
| `INSERT`, `UPDATE`, `DELETE` and other SQL | |
| `CALL other_procedure(...)` | |
| `DECLARE name CURSOR FOR select`, `OPEN`, `FETCH name INTO vars`, `CLOSE` | The cursor reads the whole result when opened |
| `DECLARE {CONTINUE \| EXIT} HANDLER FOR SQLSTATE 'xxxxx' statement` | Only the `SQLSTATE` form |
| `RETURN` | Ends the procedure |

Not supported: `LOOP`, `REPEAT`, `LEAVE`, `ITERATE`, labels, `SIGNAL`,
`RESIGNAL`, `HANDLER FOR NOT FOUND` / `SQLEXCEPTION` / named conditions.

Limitations to be aware of:

- Local variables and parameters are substituted into SQL statements **as
  text**, wherever their name occurs, including inside string literals and
  inside other identifiers. Give them names that occur nowhere else in the
  body (for example a `p_` or `v_` prefix).
- A handler's statement cannot assign to a local variable, so the usual
  "set a `done` flag in a `NOT FOUND` handler" cursor loop does not
  terminate; it ends with the iteration-limit error.

### Stored functions

```
CREATE FUNCTION name (param type, ...) RETURNS type BEGIN statements END
DROP FUNCTION [IF EXISTS] name
SHOW FUNCTION STATUS [LIKE 'pattern']
SHOW CREATE FUNCTION name
```

Functions are stored in `system.procedures`, callable from any session, in
any expression, and survive restarts. The accepted form is narrow:

- parameter types and the return type must be a single word (`INT`,
  `BIGINT`, `TEXT`; not `VARCHAR(20)`);
- the body must be `BEGIN ... END`; characteristics such as `DETERMINISTIC`
  are not accepted;
- the body may contain `DECLARE var type`, `SET var = expr`,
  `SELECT expr INTO var` and `RETURN expr`. Control flow (`IF`, `WHILE`,
  `CASE`) is not executed. A function that reaches its end without a
  `RETURN` returns `NULL`.

Creating a function whose name already exists is not refused; the newer
definition is the one used.

## DML

### SELECT

```
[WITH name AS (select) [, ...]]
SELECT [DISTINCT] select_list
[FROM table_reference [, table_reference ...]]
[WHERE condition]
[GROUP BY {expr | position}, ...]
[HAVING condition]
[ORDER BY {expr | alias | position} [ASC | DESC], ...]
[LIMIT count [OFFSET skip] | LIMIT skip, count]
[FOR UPDATE | FOR SHARE]
```

**select_list:** `*`, `table.*`, `expr [[AS] alias]`, ...

**table_reference:** `table [[AS] alias]`, a view, a derived table
`(select) AS alias` (the alias is required), or a join.

**Joins:**

- `[INNER] JOIN table ON condition`, `STRAIGHT_JOIN ... ON` (treated as inner)
- `LEFT [OUTER] JOIN table ON condition`
- `RIGHT [OUTER] JOIN table ON condition`
- `FULL [OUTER] JOIN table ON condition`
- `CROSS JOIN table`, and comma-separated tables

Equality joins are executed as hash joins, others as nested loops. `NULL`
join keys never match. `JOIN ... USING (...)` and `NATURAL JOIN` are parsed
but the join condition is **dropped**, giving a cross join; write the
condition with `ON`.

**Subqueries:** scalar subqueries, `[NOT] IN (select)` and
`[NOT] EXISTS (select)`, correlated or not, in the select list and in
`WHERE`; derived tables in `FROM`. A scalar subquery returning more than
one row is an error. `ANY` / `ALL` / `SOME` comparisons and row
constructors (`(a, b) IN (...)`, `(a, b) = (1, 2)`) are not supported.

**Set operations:** `UNION` and `UNION ALL` between top-level `SELECT`s,
with an optional final `ORDER BY` (by column name or position) and `LIMIT`.
The number of columns must match (error 1222). `UNION DISTINCT`,
`INTERSECT` and `EXCEPT` are not supported, nor is a `UNION` inside a
derived table.

**Common table expressions:** non-recursive `WITH name AS (...)`, one or
several, a later one may refer to an earlier one. `WITH RECURSIVE` and CTE
column lists (`WITH c (a, b) AS`) are not supported.

**Grouping:** `GROUP BY` expressions or select-list positions; `HAVING` may
use aggregates or select-list aliases. `WITH ROLLUP` is not supported.
Selecting a column that is neither grouped nor aggregated is not rejected
and returns meaningless values or an error; `ONLY_FULL_GROUP_BY` is not
enforced.

**Other notes:**

- `SELECT` without `FROM`, and `FROM DUAL`, evaluate expressions once.
- `ORDER BY` sorts `NULL` first ascending; `NULLS FIRST` / `NULLS LAST` are
  parsed and ignored.
- `FOR UPDATE` and `FOR SHARE` (or `LOCK IN SHARE MODE`) make the query a
  locking read: see [How transactions behave](#how-transactions-behave).
  `OF name` limits the locking to the table named, as the query names it
  (its alias, if it has one); a name the query does not have is error
  3568. Several names may be given (`OF a, b`). `NOWAIT` and
  `SKIP LOCKED` are supported.
- `SQL_CALC_FOUND_ROWS` and index hints (`USE INDEX`, `FORCE INDEX`) are
  accepted and ignored. `FOUND_ROWS()` returns the number of rows the
  previous `SELECT` returned.
- `SELECT ... INTO @var` is supported only inside stored procedures;
  `INTO OUTFILE` is not supported.
- Lookups by primary key (`WHERE pk = value`, all columns of a composite
  key) and primary-key ranges (`BETWEEN`, `<`, `>`) read only the matching
  rows; every other predicate scans the table.

### INSERT

```
INSERT [IGNORE] [INTO] table [(column, ...)] VALUES (expr | DEFAULT, ...) [, (...)]
    [ON DUPLICATE KEY UPDATE column = expr, ...]
INSERT [INTO] table SET column = expr, ...
INSERT [IGNORE] [INTO] table [(column, ...)] SELECT ...
```

- A multi-row `INSERT` is all-or-nothing.
- `INSERT IGNORE` skips rows whose primary key already exists.
- `ON DUPLICATE KEY UPDATE` applies to primary-key conflicts on
  `INSERT ... VALUES`. The update expressions may refer to the existing
  row's columns; the `VALUES(col)` function is not supported. It is not
  honoured on `INSERT ... SELECT`.
- Omitted columns take their default, or `NULL`.

### REPLACE

```
REPLACE INTO table [(column, ...)] VALUES (...) [, (...)]
```

Accepted, but incomplete: rows whose primary key does not yet exist are
inserted; rows whose primary key already exists are currently **left
unchanged**, without an error. Use `INSERT ... ON DUPLICATE KEY UPDATE`.
`REPLACE ... SET` and `REPLACE ... SELECT` behave as plain `INSERT`.

### UPDATE

```
UPDATE table SET column = expr [, ...] [WHERE condition] [LIMIT count]
```

One table, no alias. Expressions and the condition may use subqueries. The
primary key may be updated. `ORDER BY`, `UPDATE IGNORE`, `SET col = DEFAULT`
and multi-table updates are not supported.

### DELETE

```
DELETE FROM table [WHERE condition] [ORDER BY ...] [LIMIT count]
```

One table, no alias. The condition may use subqueries. `DELETE IGNORE`,
`DELETE QUICK` and multi-table deletes are not supported.

### LOAD DATA

```
LOAD DATA [LOCAL] INFILE 'path' INTO TABLE table
```

Minimal. The file is read **on the server** (also with `LOCAL`), and each
line is inserted as one row with a single value: the target must be a
one-column table. `FIELDS`, `LINES`, `IGNORE` and column-list clauses are
not interpreted; lines that cannot be inserted are skipped.

### Other statements

| Statement | Behaviour |
|-----------|-----------|
| `DO expr` | Evaluates and discards |
| `ANALYZE TABLE t` | Collects row statistics; returns a status row |
| `CHECK TABLE t`, `CHECKSUM TABLE t` | Return a fixed `OK` status row; nothing is checked |
| `FLUSH ...` | Accepted, no effect |
| `LOCK TABLES ...`, `UNLOCK TABLES` | Accepted, no effect |
| `OPTIMIZE TABLE`, `REPAIR TABLE`, `HANDLER`, `KILL`, `XA` | Not supported |

## Expressions & Operators

### Operators

| Category | Operators |
|----------|-----------|
| Arithmetic | `+` `-` `*` `/` `DIV` `%` `MOD`, unary `-` `+` |
| Comparison | `=` `!=` `<>` `<` `<=` `>` `>=` `<=>` |
| Logical | `AND` `&&` `OR` `\|\|` `XOR` `NOT` `!` |
| Bitwise | `&` `\|` `^` `~` `<<` `>>` |
| Pattern | `[NOT] LIKE` (with `ESCAPE`), `[NOT] REGEXP`, `[NOT] RLIKE` |
| Assignment | `@var := expr` |

`||` is logical OR, not concatenation. `/` yields a floating-point result
when it is not exact (`10 / 3` is `3.333333333333333`). Division or modulo
by zero yields `NULL`. Integer arithmetic that overflows 64 bits is an
error (1690).

### Other expressions

| Expression | Description |
|------------|-------------|
| `IS [NOT] NULL` | Null test |
| `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN` | Truth tests |
| `[NOT] IN (value, ...)`, `[NOT] IN (select)` | Membership |
| `[NOT] BETWEEN x AND y` | Inclusive range |
| `[NOT] EXISTS (select)`, `(select)` | Subqueries |
| `CASE WHEN c THEN r ... [ELSE r] END`, `CASE x WHEN v THEN r ... END` | |
| `CAST(expr AS type)`, `CONVERT(expr, type)` | Types: `SIGNED`, `UNSIGNED`, `CHAR[(n)]`, `BINARY`, `DECIMAL(p,s)`, `DOUBLE`, `FLOAT`, `DATE`, `DATETIME`, `TIME`, `JSON` |
| `CONVERT(expr USING charset)`, `expr COLLATE name`, `_charset'text'` | Accepted; the charset or collation is ignored |
| `BINARY expr` | Cast to binary |
| `expr + INTERVAL n unit`, `expr - INTERVAL n unit` | Units: `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR` |
| `EXTRACT(unit FROM expr)` | Single units only |
| `SUBSTRING(s FROM p [FOR n])`, `TRIM([LEADING \| TRAILING \| BOTH] [x FROM] s)` | Standard forms |
| `json -> 'path'`, `json ->> 'path'` | `JSON_EXTRACT`, and `JSON_UNQUOTE(JSON_EXTRACT(...))` |
| `@name` | User variable; `NULL` if never set |

Not supported: row constructors and tuple comparison, `ANY` / `ALL`,
`POSITION(x IN y)` (use `LOCATE` or `POSITION(x, y)`), compound interval
units (`DAY_HOUR`, `HOUR_MINUTE`, ...).

### Comparison rules

- **Strings compare byte-wise and case-sensitively**, in `=`, `<`, `IN`,
  `LIKE`, `ORDER BY`, `GROUP BY` and `DISTINCT` (`'a' = 'A'` is false,
  `'B' < 'a'` is true). Trailing spaces are significant. `COLLATE` has no
  effect.
- **A number is not converted for comparison with a string**:
  `1 = '1'` is false and `WHERE int_column = '1'` matches nothing. Compare
  like with like, or `CAST`. (Arithmetic does convert: `1 + '1'` is `2`.)
- A primary-key lookup matches only when the literal has the key's type:
  `WHERE int_pk = 1.0` finds no row.
- `LIKE`: `%` matches any run of characters, `_` one character; `\` or the
  `ESCAPE` character quotes them.

### NULL semantics

- Arithmetic and comparison with `NULL` give `NULL`; `<=>` treats two
  `NULL`s as equal.
- `FALSE AND NULL` is `FALSE`, `TRUE AND NULL` is `NULL`; `TRUE OR NULL` is
  `TRUE`, `FALSE OR NULL` is `NULL`.
- `x IN (...)` is `NULL` when `x` is `NULL`, or when nothing matches and the
  list contains `NULL`. `BETWEEN` with a `NULL` operand is `NULL`.
- Aggregates ignore `NULL`s; `COUNT(*)` counts rows. `NULL`s form one group
  in `GROUP BY` and are equal for `DISTINCT`.

## Literals

| Type | Syntax |
|------|--------|
| Null | `NULL` |
| Boolean | `TRUE` `FALSE` |
| Integer | `123` `-456` |
| Decimal / float | `1.5` `.5` `1e10` `1.5e-2` |
| String | `'text'` `"text"`; `''` or `\'` for a quote; adjacent literals concatenate |
| Hexadecimal | `X'41'` `0x41` (a byte string; a number in arithmetic) |
| Bit | `b'101'` |
| Date and time | `DATE '2024-01-02'`, `TIME '03:04:05'`, `TIMESTAMP '2024-01-02 03:04:05'`, or plain strings in those formats |

`N'text'` and `0b101` are not accepted.

## Functions

Calling a name that is neither listed here nor a stored function is an
error (`unknown function`). Descriptions are given where the behaviour is
not the MySQL function of the same name in the obvious way.

### Control flow and NULL handling

`IF(c, a, b)`, `IFNULL(a, b)`, `NULLIF(a, b)`, `COALESCE(a, ...)`,
`ISNULL(a)`, `GREATEST(a, ...)`, `LEAST(a, ...)`

### String

| Functions | |
|-----------|---|
| `CONCAT`, `CONCAT_WS` | `CONCAT` is `NULL` if any argument is `NULL` |
| `LENGTH` / `LEN` / `OCTET_LENGTH`, `CHAR_LENGTH` / `CHARACTER_LENGTH`, `BIT_LENGTH` | Bytes, characters, bits |
| `UPPER` / `UCASE`, `LOWER` / `LCASE` | |
| `SUBSTRING` / `SUBSTR` / `MID`, `SUBSTRING_INDEX`, `LEFT`, `RIGHT` | |
| `TRIM`, `LTRIM`, `RTRIM`, `LPAD`, `RPAD`, `SPACE`, `REPEAT`, `REVERSE` | |
| `REPLACE`, `INSERT` | |
| `LOCATE` / `POSITION`, `INSTR`, `FIELD`, `ELT`, `FIND_IN_SET`, `MAKE_SET`, `EXPORT_SET` | |
| `STRCMP`, `REGEXP(s, pattern)` | |
| `ASCII` / `ORD`, `CHAR`, `HEX`, `UNHEX`, `BIN`, `OCT`, `CONV` | |
| `FORMAT`, `QUOTE`, `WEIGHT_STRING` | |
| `TO_BASE64`, `FROM_BASE64` | |

### Numeric

`ABS`, `SIGN`, `MOD`, `CEIL` / `CEILING`, `FLOOR`, `ROUND`, `TRUNCATE`,
`POW` / `POWER`, `SQRT`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`, `PI`,
`RADIANS`, `DEGREES`, `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN` /
`ATAN2`, `RAND`, `CRC32`, `BIT_COUNT`

### Date and time

| Functions | |
|-----------|---|
| `NOW` / `CURRENT_TIMESTAMP` / `SYSDATE` / `LOCALTIME` / `LOCALTIMESTAMP`, `CURDATE` / `CURRENT_DATE`, `CURTIME` / `CURRENT_TIME` | Current date and time |
| `UTC_DATE`, `UTC_TIME`, `UTC_TIMESTAMP`, `UNIX_TIMESTAMP`, `FROM_UNIXTIME` | |
| `DATE`, `TIME`, `YEAR`, `QUARTER`, `MONTH`, `DAY` / `DAYOFMONTH`, `HOUR`, `MINUTE`, `SECOND`, `MICROSECOND` | Parts |
| `DAYOFWEEK`, `DAYOFYEAR`, `WEEKDAY`, `DAYNAME`, `MONTHNAME`, `WEEK`, `WEEKOFYEAR`, `YEARWEEK`, `LAST_DAY` | |
| `DATE_ADD` / `ADDDATE`, `DATE_SUB` / `SUBDATE`, `ADDTIME`, `SUBTIME`, `TIMESTAMPADD` | With `INTERVAL n unit` |
| `DATEDIFF`, `TIMEDIFF`, `TIMESTAMPDIFF`, `PERIOD_ADD`, `PERIOD_DIFF` | |
| `DATE_FORMAT`, `TIME_FORMAT`, `STR_TO_DATE`, `GET_FORMAT` | |
| `TO_DAYS`, `FROM_DAYS`, `TO_SECONDS`, `SEC_TO_TIME`, `TIME_TO_SEC`, `MAKEDATE`, `MAKETIME`, `CONVERT_TZ` | |

### JSON

`JSON_ARRAY`, `JSON_OBJECT`, `JSON_QUOTE`, `JSON_UNQUOTE`, `JSON_EXTRACT`,
`JSON_VALUE`, `JSON_KEYS`, `JSON_TYPE`, `JSON_VALID`, `JSON_LENGTH`,
`JSON_DEPTH`, `JSON_PRETTY`, `JSON_STORAGE_SIZE`, `JSON_STORAGE_FREE`,
`JSON_SET`, `JSON_INSERT`, `JSON_REPLACE`, `JSON_REMOVE`,
`JSON_ARRAY_APPEND`, `JSON_ARRAY_INSERT`, `JSON_MERGE_PRESERVE`,
`JSON_MERGE_PATCH`, `JSON_CONTAINS`, `JSON_CONTAINS_PATH`, `JSON_SEARCH`,
`JSON_OVERLAPS`, `JSON_SCHEMA_VALID`, `JSON_SCHEMA_VALIDATION_REPORT` (the
last two check only the schema's `type`)

### Hashing, encryption, compression

`MD5`, `SHA` / `SHA1`, `SHA2`, `AES_ENCRYPT`, `AES_DECRYPT`,
`RANDOM_BYTES`, `COMPRESS`, `UNCOMPRESS`, `UNCOMPRESSED_LENGTH`

### Network and identifiers

`INET_ATON`, `INET_NTOA`, `INET6_ATON`, `INET6_NTOA`, `IS_IPV4`, `IS_IPV6`,
`UUID`, `UUID_SHORT`

### Spatial

`ST_GeomFromText` / `ST_GeometryFromText`, `ST_GeometryFromWKB`,
`ST_PointFromWKB`, `ST_LineStringFromWKB`, `ST_PolyFromWKB` /
`ST_PolygonFromWKB`, `ST_X`, `ST_Y`, `ST_NumPoints`, `ST_Length`,
`ST_Area`. There is no `ST_AsText`, `ST_Distance` or `POINT()`.

### Session and information

| Function | |
|----------|---|
| `DATABASE()` / `SCHEMA()` | Selected database |
| `USER()`, `CURRENT_USER()`, `SESSION_USER()`, `SYSTEM_USER()` | |
| `VERSION()` | `8.0.0-RooDB` |
| `CONNECTION_ID()` | |
| `LAST_INSERT_ID()` | Last `AUTO_INCREMENT` value generated in this session |
| `ROW_COUNT()` | Rows affected by the previous statement |
| `FOUND_ROWS()` | Rows returned by the previous `SELECT` |
| `SLEEP(n)` | Returns `0` immediately; does not wait |
| `BENCHMARK(n, expr)` | Returns `0` |
| `GET_LOCK`, `RELEASE_LOCK`, `IS_FREE_LOCK` | Always succeed; no lock is taken |

### Aggregate

| Function | |
|----------|---|
| `COUNT(*)`, `COUNT([DISTINCT] expr)` | |
| `SUM([DISTINCT] expr)`, `AVG([DISTINCT] expr)` | |
| `MIN(expr)`, `MAX(expr)` | |
| `GROUP_CONCAT(expr [SEPARATOR 'sep'])` | `ORDER BY` inside the call is ignored |
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | |
| `STD` / `STDDEV` / `STDDEV_SAMP`, `STDDEV_POP`, `VARIANCE` / `VAR_POP`, `VAR_SAMP` | `STD` and `STDDEV` compute the sample deviation |
| `ANY_VALUE(expr)` | |
| `JSON_ARRAYAGG(expr)`, `JSON_OBJECTAGG(key, value)` | |

### Window

```
function(...) OVER ([PARTITION BY expr, ...] [ORDER BY expr [ASC | DESC], ...])
```

`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE(n)`, `LAG`, `LEAD`,
`FIRST_VALUE`, `LAST_VALUE`, and the aggregates `SUM`, `COUNT`, `AVG`,
`MIN`, `MAX`.

Frames are not supported: an aggregate with `OVER` is always computed over
the whole partition, also when `ORDER BY` or a `ROWS` / `RANGE` clause is
given (there are no running totals). Named windows (`WINDOW w AS`) are not
supported. Other window functions (`PERCENT_RANK`, `CUME_DIST`,
`NTH_VALUE`) return `NULL`.

## Transaction Control

| Statement | Description |
|-----------|-------------|
| `BEGIN`, `START TRANSACTION` | Start a transaction. If one is open it is committed first. (Over the TDS endpoint transactions nest instead, as in T-SQL: see [TDS driver shims](#tds-driver-shims).) Modifiers such as `READ ONLY` and `WITH CONSISTENT SNAPSHOT` are accepted and ignored |
| `COMMIT` | Commit. Without an open transaction, no effect |
| `ROLLBACK` | Discard the transaction's changes |
| `SET autocommit = {0 \| 1 \| ON \| OFF}` | See below |
| `SET [SESSION] TRANSACTION ISOLATION LEVEL level` | `READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`; not inside a transaction. `SERIALIZABLE` is refused |

`BEGIN WORK`, `COMMIT WORK`, `ROLLBACK WORK`, `AND CHAIN`, savepoints
(`SAVEPOINT`, `ROLLBACK TO`, `RELEASE SAVEPOINT`) and `XA` are not
supported.

### How transactions behave

- **Writes are buffered until `COMMIT`.** Other sessions never see
  uncommitted changes. `COMMIT` applies all of a transaction's changes
  atomically, or none.
- **Every statement reads a fresh snapshot.** The reported isolation level
  is `REPEATABLE-READ`, but inside a transaction each `SELECT` sees
  everything committed before that statement started, so reads behave as
  READ COMMITTED: a row read twice in one transaction can differ, and new
  rows can appear. Setting another isolation level does not change this.
- **Writers lock rows and wait for each other.** `UPDATE` and `DELETE` take
  an exclusive lock on each row they examine; `INSERT` into a table with a
  primary key locks the new row's key. A statement that needs a row locked
  by another transaction waits until that transaction commits or rolls
  back, then works on the row as it then stands (so `v = v + 1` from two
  sessions adds two). Locks are held until the transaction ends. A plain
  `SELECT` takes no locks and never waits.
  - Rows are locked as they are scanned: an `UPDATE` or `DELETE` that
    cannot find its rows by primary key examines, and locks, every row of
    the table.
  - A statement that reads a range also waits for rows other transactions
    are inserting into it, and for an `UPDATE` that moves a row into it by
    changing its primary key. A deadlock inside a stored procedure or
    trigger is a deadlock like any other: error 1213, transaction rolled
    back.
  - **What was read stays as it was** (`REPEATABLE READ`, the default).
    Besides the rows, the statement locks the gaps it read against
    inserts: a scan locks the whole table, so no row can be added until
    the transaction ends; a primary-key lookup that finds nothing locks
    the gap between the neighbouring rows, so the missing row (or any
    other in that gap) cannot be inserted by someone else. (A recently
    deleted row still counts as a neighbour.) Gap locks never
    make another reader of the same gap wait; they only make inserts wait.
  - Under `READ COMMITTED` there are no gap locks, and a row that was
    examined but did not match the `WHERE` is unlocked again at once.
  - **Locking reads.** `SELECT ... FOR UPDATE` locks the rows it reads
    exclusively, `FOR SHARE` in shared mode (other `FOR SHARE` readers do
    not wait; writers do). A locking read waits for rows another
    transaction has locked and returns the latest committed rows, not the
    statement's snapshot; it locks the range it read like `UPDATE` does.
    With `NOWAIT` it fails at once (error 3572) instead of waiting; with
    `SKIP LOCKED` it leaves locked rows out of the result. Rows are locked
    where the tables are read: rows that a `WHERE` on a non-key column
    rejects may be locked too. Outside a transaction the locks last only
    until the statement returns.
  - `LOCK TABLES` is a no-op.
- **A duplicate key is reported by the statement.** An `INSERT` whose
  primary key already exists fails with error 1062 when it runs. If the
  existing row belongs to a transaction that has not finished, the insert
  waits to see whether it commits.
- **Deadlock.** If transactions wait for each other in a cycle, the one
  whose request would close the cycle fails with error 1213 (SQLSTATE
  40001) and its whole transaction is rolled back. Retry it.
- **Lock wait timeout.** A statement that waits longer than
  `innodb_lock_wait_timeout` seconds (default 50; `SET [SESSION | GLOBAL]
  innodb_lock_wait_timeout = n`; the global form needs `root`) fails with
  error 1205. Only that statement
  fails; the transaction stays open.
- **A conflict can still be found at `COMMIT`** when a transaction changes
  a row it did not lock at the time, which today means a row it inserted
  itself (see the next point). The commit then fails with error 1213 and
  the transaction is rolled back.
- **A transaction's own writes are partly visible to it.** `SELECT` sees
  rows the transaction has inserted or changed. `UPDATE` and `DELETE` see
  the transaction's earlier changes to rows that already existed, but do
  not find rows the same transaction inserted.
- **In a cluster, only the primary writes.** On any other node a session
  is read-only: `SELECT` works (from that node's own state, which may be a
  little behind the primary), and every statement that writes or locks,
  DDL and `SELECT ... FOR UPDATE` included, fails with error 1290. If the
  primary changes while a transaction is open, the transaction is rolled
  back and its `COMMIT` fails with error 3101. See
  [admin.md](admin.md#deployment-modes).
- **A session that ends takes its transaction with it.** When a client
  disconnects, the connection fails, or the connection is reset, the open
  transaction is rolled back and the session's temporary tables are
  dropped.
- **DDL is not transactional.** `CREATE`, `DROP` and `ALTER` take effect
  immediately, are not undone by `ROLLBACK`, and do not commit the open
  transaction.
- **`autocommit`.** By default each statement commits on its own. After
  `SET autocommit = 0` every statement runs in a transaction: the first
  statement opens one, it lasts until `COMMIT` or `ROLLBACK`, and the next
  statement opens another. `SET autocommit = 1` commits the open
  transaction. `@@autocommit` reports the session's setting.

## Users and Privileges

Accounts are rows of `system.users`; grants are rows of `system.grants`.
The `root` account is created by `roodb_init` (see [admin.md](admin.md))
and bypasses all privilege checks.

| Statement | State |
|-----------|-------|
| `CREATE ROLE [IF NOT EXISTS] name` | Adds an account `name`@`%` with an empty password. Repeating it adds a duplicate row. Such an account is currently refused at login |
| `GRANT privilege [, ...] ON object TO 'user'@'host' [WITH GRANT OPTION]` | Records the grant. Privileges: `ALL [PRIVILEGES]`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`. Objects: `*.*`, `db.*`, `db.table`. The grantee need not exist |
| `REVOKE privilege [, ...] ON object FROM 'user'@'host'` | Parsed; does not reliably remove the grant |
| `CREATE USER`, `ALTER USER`, `DROP USER`, `DROP ROLE`, `SHOW GRANTS` | Not supported (parse or "unsupported" error); no statement sets a password |
| `SET PASSWORD ...` | Accepted, **no effect** |
| `GRANT` of `DROP`, `ALTER`, `INDEX` and other privileges | Not supported |

In practice, `root` is the only account that can be used. For a non-root
session the server checks `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX` and `DROP INDEX` against
`system.grants`, with the host pattern matched against the client address;
other statements (views, procedures, functions, triggers, `ALTER TABLE`,
`CREATE/DROP DATABASE`, `GRANT` itself, `LOAD DATA`) are not checked.

## SHOW, DESCRIBE, EXPLAIN

| Statement | Result |
|-----------|--------|
| `SHOW DATABASES`, `SHOW SCHEMAS` | Database names |
| `SHOW TABLES`, `SHOW FULL TABLES` | All tables and views (one column) |
| `SHOW TABLES {FROM \| IN} db` | All tables |
| `SHOW CREATE TABLE name` | Reconstructed `CREATE TABLE` (stored types, primary key), or the view definition |
| `DESCRIBE t`, `DESC t`, `SHOW [FULL] {COLUMNS \| FIELDS} FROM t` | `Field`, `Type`, `Null`, `Key`, `Default`, `Extra`; a `LIKE` clause is ignored |
| `SHOW {INDEX \| INDEXES \| KEYS} FROM t` | Recorded indexes and the primary key |
| `SHOW PROCEDURE STATUS`, `SHOW FUNCTION STATUS` `[LIKE 'pattern']` | |
| `SHOW CREATE PROCEDURE name`, `SHOW CREATE FUNCTION name` | |
| `SHOW WARNINGS`, `SHOW ERRORS`, `SHOW COUNT(*) WARNINGS` | Accepted; the list is normally empty |
| `SHOW ENGINES`, `SHOW COLLATION`, `SHOW CHARACTER SET` / `SHOW CHARSET` | One fixed row each (`RooDB`, `utf8mb4_general_ci`, `utf8mb4`) |
| `SHOW [GLOBAL \| SESSION] VARIABLES`, `SHOW [GLOBAL \| SESSION] STATUS`, `SHOW [FULL] PROCESSLIST` | Always empty |
| `EXPLAIN statement` | A MySQL-style plan table. `type` is `const` for a primary-key lookup, `range` for a primary-key range, `ALL` for a scan. `ANALYZE` and `FORMAT=` are ignored |

Not supported: `SHOW TABLES LIKE`, `SHOW TABLE STATUS`, `SHOW TRIGGERS`,
`SHOW CREATE VIEW`, `SHOW CREATE TRIGGER`, `SHOW CREATE DATABASE`,
`SHOW GRANTS`, `SHOW PRIVILEGES`, `SHOW PLUGINS`, `EXPLAIN table`.

A query mentioning `INFORMATION_SCHEMA` or `PERFORMANCE_SCHEMA` is answered
with a fixed result, for client compatibility: `INFORMATION_SCHEMA.TABLES`
(all tables; the query's own column list and `WHERE` are ignored),
`SCHEMATA`, `ENGINES`, empty `COLUMNS` and `PLUGINS`, and an empty result
for anything in `PERFORMANCE_SCHEMA`. Use the [system tables](#system-tables)
for real metadata.

## Variables

### User variables

```
SET @name = expr [, @name2 = expr2 ...]
SELECT @name, @name := expr
```

Per session; an unset variable is `NULL`.

### SET

| Statement | Behaviour |
|-----------|-----------|
| `SET autocommit = ...` | See [Transaction Control](#transaction-control) |
| `SET [SESSION] TRANSACTION ISOLATION LEVEL ...` | See [Transaction Control](#transaction-control) |
| `SET [SESSION \| GLOBAL] sql_mode = '...'` | Stored for the session and returned by `@@sql_mode`. Only `NO_UNSIGNED_SUBTRACTION` (unsigned subtraction may go negative), `ERROR_FOR_DIVISION_BY_ZERO` and `STRICT_TRANS_TABLES` are consulted, and the latter two only in a few numeric conversions. `ANSI_QUOTES` has no effect |
| `SET NAMES ...`, `SET CHARACTER SET ...` | Accepted, no effect |
| Any other `SET ...` (including unknown variables, `SET GLOBAL ...`, `SET PASSWORD ...`) | Accepted, **no effect** |

### System variables

A statement that contains `@@name` is answered with one row holding the
value of each `@@` variable it mentions, and nothing else: the rest of the
statement is not evaluated (`SELECT 1, @@version` returns one column). This
also applies to `@@` inside a string literal. `@@global.` and `@@session.`
prefixes are accepted.

| Variable | Value |
|----------|-------|
| `version` | `8.0.0-RooDB` |
| `version_comment` | `RooDB` |
| `sql_mode` | Session value; initially `ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES` |
| `autocommit` | Session value |
| `innodb_lock_wait_timeout` | Session value, seconds; initially 50 |
| `transaction_isolation`, `tx_isolation` | `REPEATABLE-READ` (fixed) |
| `max_allowed_packet` | `16777216` |
| `character_set_client`, `character_set_connection`, `character_set_results` | `utf8mb4` |
| `collation_connection` | `utf8mb4_general_ci` |
| `time_zone` | `SYSTEM` |
| `system_time_zone` | `UTC` |
| `default_storage_engine`, `storage_engine` | `RooDB` |
| `lower_case_table_names` | `0` |
| `have_ssl`, `have_openssl` | `YES` |
| `wait_timeout`, `interactive_timeout` | `28800` |
| `net_write_timeout` | `60` |
| `net_read_timeout` | `30` |
| `socket` | `/tmp/roodb.sock` |

Unknown variables return an empty string. Apart from `sql_mode` these are
fixed compatibility values; setting them changes nothing.

## Prepared Statements

### Protocol level

- `COM_STMT_PREPARE` — parse the statement, return a statement id and
  parameter count
- `COM_STMT_EXECUTE` — bind `?` parameters and execute
- `COM_STMT_CLOSE` — release the statement
- `COM_STMT_RESET` — accepted
- `COM_RESET_CONNECTION` — rolls back, drops temporary tables and prepared
  statements, resets session settings

`SELECT`, `INSERT`, `UPDATE` and `DELETE` can be prepared with parameters,
as can `BEGIN`, `COMMIT`, `ROLLBACK` and `SET` without parameters (a `SET`
with parameters is error 1295). Statements that the server handles outside
its planner (`SHOW`, `USE`, `CALL`, `ALTER TABLE`, and the like) should be
sent as plain queries. The plan is cached per statement and rebuilt when
the schema changes.

### SQL level

```
PREPARE name FROM 'statement text'
EXECUTE name [USING @var, ...]
{DEALLOCATE | DROP} PREPARE name
```

Each `?` is replaced, in order, by the value of the corresponding user
variable. The text must be a quoted literal (not a variable).

## System Tables

Schema and accounts are stored in `system.*` tables, which can be queried
with `SELECT`:

| Table | Columns |
|-------|---------|
| `system.tables` | `table_name` |
| `system.columns` | `table_name`, `column_name`, `ordinal`, `data_type`, `nullable`, `default_value`, `auto_increment` |
| `system.indexes` | `index_name`, `table_name`, `columns`, `is_unique` |
| `system.constraints` | `table_name`, `ordinal`, `constraint_type` (`PRIMARY_KEY`, `UNIQUE`, `FOREIGN_KEY`, `CHECK`), `columns`, `ref_table`, `ref_columns`, `check_expr`, `constraint_name` |
| `system.views` | `view_name`, `query_sql` |
| `system.triggers` | `trigger_name`, `table_name`, `create_sql` |
| `system.procedures` | `procedure_name`, `params_json`, `body_sql`, `returns_type` (set for functions) |
| `system.databases` | `database_name` |
| `system.table_statistics` | `table_name`, `row_count`, `data_size`, `last_analyzed` |
| `system.users` | `username`, `host`, `password_hash`, `auth_plugin`, `ssl_subject`, `ssl_issuer`, `account_locked`, `password_expired`, `created_at`, `updated_at` |
| `system.grants` | `grantee`, `grantee_host`, `grantee_type`, `privilege`, `object_type`, `database_name`, `table_name`, `with_grant_option`, `granted_by`, `granted_at` |
| `system.roles` | `role_name`, `created_at` |
| `system.role_grants` | `username`, `host`, `role_name`, `with_admin_option`, `granted_at` |

Temporary tables are not listed in `system.tables`. The system tables are
not protected against `INSERT`, `UPDATE` and `DELETE`; do not write to
them directly.

## TDS driver shims

The server can also accept SQL Server (TDS) clients on a second port; see
[admin.md](admin.md#sql-server-protocol-tds). The SQL dialect on that port
is the one described here; T-SQL is not translated. Only what drivers send
by themselves is understood in addition:

| Sent by the driver | Handling |
|--------------------|----------|
| Several statements in one batch | Split at top-level `;` and run in order (a `CREATE PROCEDURE` / `FUNCTION` / `TRIGGER` is not split) |
| `BEGIN TRAN[SACTION] [name]` | `BEGIN` |
| `COMMIT [TRAN[SACTION] [name] \| WORK]` | `COMMIT` |
| `ROLLBACK [TRAN[SACTION] \| WORK]` | `ROLLBACK` |
| `SAVE TRAN[SACTION] name`, `ROLLBACK TRAN[SACTION] name` | Refused: savepoints are not supported |
| `IF @@TRANCOUNT > 0 statement` | Runs the statement only inside a transaction |
| Nested `BEGIN TRAN` | Adds a level (`@@TRANCOUNT` counts them). An inner `COMMIT` only closes its level; the outermost commits. `ROLLBACK` at any level undoes the whole transaction |
| `SET IMPLICIT_TRANSACTIONS {ON \| OFF}` | `SET autocommit = 0` / `1` |
| `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` | `REPEATABLE READ` |
| `SET ANSI_DEFAULTS`, `ANSI_NULLS`, `ANSI_NULL_DFLT_ON`, `ANSI_NULL_DFLT_OFF`, `ANSI_PADDING`, `ANSI_WARNINGS`, `ARITHABORT`, `ARITHIGNORE`, `CONCAT_NULL_YIELDS_NULL`, `CURSOR_CLOSE_ON_COMMIT`, `DATEFIRST`, `DATEFORMAT`, `DEADLOCK_PRIORITY`, `LANGUAGE`, `LOCK_TIMEOUT`, `NOCOUNT`, `NO_BROWSETABLE`, `NUMERIC_ROUNDABORT`, `QUERY_GOVERNOR_COST_LIMIT`, `QUOTED_IDENTIFIER`, `TEXTSIZE`, `XACT_ABORT` | Accepted, no effect |
| `SET FMTONLY OFF`, `SET NOEXEC OFF`, `SET ROWCOUNT 0` | Accepted; turning these on is refused |
| `@@SPID`, `@@TRANCOUNT`, `@@MAX_PRECISION`, `@@SERVERNAME`, `@@LANGUAGE`, `@@MICROSOFTVERSION`, `@@VERSION` | Replaced by a value |
| `DB_NAME()`, `SUSER_SNAME()`, `SUSER_NAME()`, `ORIGINAL_LOGIN()`, `SCHEMA_NAME()`, `SERVERPROPERTY('name')` | Replaced by a value (`NULL` for an unknown property) |

## Limits

| Limit | Value |
|-------|-------|
| Key of a row (encoded primary key plus the table prefix) | 65,535 bytes |
| One row | 1 GiB |
| Temporary-table rows per session | 256 MiB |
| `WHILE` iterations in a stored procedure | 100,000 |
| Statements per request | 1 |
| `BIT` width | 64 |
| `DECIMAL` precision / scale | 65 / 30 |

A statement that would store a row beyond the key or row limit fails with
an error ("Key too large" / "Value too large") and changes nothing.

## Errors

Errors carry a MySQL error number and an SQLSTATE. The ones the server
returns most often:

| Code | SQLSTATE | Meaning |
|------|----------|---------|
| 1045 | 28000 | Access denied (login or privilege) |
| 1048 | 23000 | Column cannot be `NULL` |
| 1049 | HY000 | Unknown database |
| 1050 | 42S01 | Table already exists |
| 1051 | 42S02 | `DROP TABLE` of an unknown table |
| 1054 | 42S22 | Unknown column |
| 1062 | 23000 | Duplicate primary key |
| 1064 | 42000 | Syntax error |
| 1065 | HY000 | Empty query |
| 1105 | HY000 | General error. Used for unsupported statements and expressions, and most errors in views, procedures, functions and triggers |
| 1136 | 21S01 | Column count does not match value count |
| 1205 | HY000 | Lock wait timeout exceeded |
| 1213 | 40001 | Deadlock, or a conflict found at `COMMIT`; the transaction was rolled back |
| 1146 | 42S02 | Table does not exist |
| 1222 | 21000 | `UNION` branches have different numbers of columns |
| 1264, 1690 | 22003 | Numeric value out of range. 1690 is also reported for errors raised while a result is being returned, such as an unknown function |
| 1292 | 22007 | Truncated incorrect value |
| 1290 | HY000 | The node is a secondary of its cluster: it is read-only |
| 1295 | HY000 | Statement cannot be prepared with parameters |
| 3101 | 40000 | The transaction was rolled back because its node stopped being the primary; start it again on the primary |
| 3568 | HY000 | `FOR UPDATE OF` / `FOR SHARE OF` names a table the query does not have |
| 3572 | HY000 | A row was locked and the statement said `NOWAIT` |
| 1568 | HY000 | Isolation level changed inside a transaction |
| 1063, 1074, 1425, 1427 | 42000 | Invalid column type specification |

Several conditions that have a dedicated code in MySQL (unknown procedure,
unknown function, duplicate view or procedure) are reported as 1105.

## Limitations and differences from MySQL

**Data integrity**

- Only `PRIMARY KEY` is enforced. `UNIQUE`, `FOREIGN KEY` and `CHECK` are
  recorded but not checked.
- `NOT NULL` is not enforced by `UPDATE`, multi-row `INSERT` or
  `INSERT IGNORE` (the value becomes the default or zero).
- Declared type sizes are not enforced (integer ranges, `VARCHAR` length,
  `DECIMAL` scale); `ENUM` and `SET` are plain text; `sql_mode` strictness
  does not change this.
- Secondary indexes are metadata: not used by queries, uniqueness not
  enforced.
- `ALTER TABLE` does not rewrite existing rows (see [ALTER TABLE](#alter-table)).
- `REPLACE` does not replace existing rows.
- A row whose encoded primary key is larger than 65,535 bytes is refused
  (MySQL limits key length per index instead).

**Transactions**

- Reads are READ COMMITTED in effect (a snapshot per statement) although
  `REPEATABLE-READ` is reported; `SERIALIZABLE` is refused.
- Row and gap locks are taken by `UPDATE`, `DELETE`, `INSERT ... VALUES`
  (and so by `REPLACE` and `LOAD DATA`, which insert that way) and by
  locking reads. `LOCK TABLES` and `GET_LOCK` lock nothing, and
  `INSERT ... SELECT` neither takes locks nor waits for them (a conflict
  is found at `COMMIT`).
- `UPDATE` and `DELETE` do not find rows the same transaction inserted.
- No savepoints, no `XA`, no `COMMIT WORK` / `AND CHAIN`.
- DDL is neither transactional nor an implicit commit.

**Queries**

- String comparison is case-sensitive and byte-wise; there are no
  collations.
- Numbers and strings are not converted when compared (`1 = '1'` is false).
- `JOIN ... USING` and `NATURAL JOIN` lose their join condition.
- No `INTERSECT`, `EXCEPT`, `UNION DISTINCT`, recursive CTEs, `WITH ROLLUP`,
  `ANY` / `ALL`, row constructors, window frames or named windows.
- `GROUP_CONCAT(... ORDER BY ...)`, `NULLS FIRST/LAST` and index hints are
  parsed and ignored. `ONLY_FULL_GROUP_BY` is not enforced.
- `UPDATE` and `DELETE` are single-table, without aliases; `UPDATE` has no
  `ORDER BY`.
- `/` is floating-point; division by zero gives `NULL`.
- `SLEEP()` does not wait.
- One statement per request.

**Schema objects and names**

- One table namespace; databases are labels only and `db.table` does not
  work.
- Table names cannot be backtick-quoted and are case-sensitive.
- Only `INSERT ... VALUES` fires triggers; `UPDATE` and `DELETE` triggers
  never fire; trigger bodies are one statement; `BEFORE` triggers run after
  the insert and cannot modify the row.
- Procedures lack `LOOP`, `REPEAT`, `LEAVE`, `SIGNAL` and condition
  handlers other than `SQLSTATE`; variable substitution is textual.
- Functions accept only single-word types and a `BEGIN ... END` body
  without control flow.
- Views are read-only and are not validated when created.
- No `RENAME TABLE`, `CREATE TABLE ... LIKE`, generated columns,
  `DEFAULT CURRENT_TIMESTAMP`, `ON UPDATE`, partitioning, `FULLTEXT` or
  `SPATIAL` indexes, events.
- `AUTO_INCREMENT` values are unique and increasing but not consecutive and
  not per table.

**Administration through SQL**

- No working `CREATE USER`, `ALTER USER`, `DROP USER`, `SET PASSWORD` or
  `SHOW GRANTS`; `root` is the only usable account.
- Unknown `SET` statements succeed without effect; system variables other
  than `sql_mode` are fixed values, and a statement containing `@@` returns
  only those variables.
- `SHOW VARIABLES` and `SHOW PROCESSLIST` are empty; `SHOW STATUS` reports
  only lock counts (`Innodb_row_lock_current_waits`, `Roodb_row_locks`,
  `Roodb_gap_locks`);
  `INFORMATION_SCHEMA` is a small fixed emulation.
- `LOAD DATA` reads server-side files into single-column tables only.
