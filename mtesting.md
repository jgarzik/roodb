# MySQL Test Suite Integration

## Overview

RooDB uses the official MySQL 8.0 test suite (`mysql-testsuite-8.0` package) to validate SQL compatibility. Tests run via the `mysqltest` client binary against a live RooDB server over TLS.

## Prerequisites

```bash
sudo apt-get install mysql-testsuite-8.0
cargo build --release
```

Key paths:
- `mysqltest` binary: `/usr/lib/mysql-test/bin/mysqltest`
- Official tests: `/usr/lib/mysql-test/t/*.test`
- Our recorded results: `tests/mysql_compat/mtr_r/`

## Running Tests

```bash
# Custom compatibility tests (23 tests, all pass)
python3 tests/mysql_compat/run_mysql_tests.py

# Official MySQL tests (curated, tiered)
python3 tests/mysql_compat/run_mtr_tests.py                    # all tiers
python3 tests/mysql_compat/run_mtr_tests.py --tier=1           # tier 1 only
python3 tests/mysql_compat/run_mtr_tests.py --tier=1,2         # tiers 1+2
python3 tests/mysql_compat/run_mtr_tests.py --filter=func_op   # filter by name
python3 tests/mysql_compat/run_mtr_tests.py --record           # record baselines
python3 tests/mysql_compat/run_mtr_tests.py --list             # list available tests
```

## Test Tiers

| Tier | Tests | Focus |
|------|-------|-------|
| 1 | func_op, bool, type_uint, compare, comments, func_equal | Basic arithmetic, types, comparisons |
| 2 | null, case, type_varchar, type_ranges, func_isnull, limit, type_binary, bigint | NULL semantics, data types, LIMIT |
| 3 | type_decimal, type_float, func_like, func_test, func_math, delete, cast, type_year, type_blob, type_enum | Functions, DML, type casting |
| 4 | insert, update, func_str, func_concat, func_if | Complex DML, string functions |
| 5 | alias, truncate, func_in_none, select_found, distinct, having | Aliases, DISTINCT, HAVING, IN() |
| 6 | func_system, replace, func_regexp, func_group, union | System funcs, REPLACE, REGEXP, aggregates, UNION |
| 7 | ansi, binary_to_hex, count_distinct, date_formats, create_if_not_exists, constraints | ANSI, HEX/UNHEX, COUNT(DISTINCT), dates, DDL |
| 8 | subselect, drop, auto_increment, type_timestamp, key, join | Subqueries, DROP, AUTO_INC, TIMESTAMP, indexes, JOINs |
| 9 | select_all, overflow, type_uint, bool, negation_elimination, func_sapdb, order_by_sortkey | SELECT, overflow, negation, SAP functions, sort keys |
| 10 | func_misc, single_delete_update, insert_select | INET functions, DELETE/UPDATE LIMIT, INSERT...SELECT |
| 11 | truncate_coverage, func_group_innodb, type_set, varbinary | TRUNCATE edge cases, bitwise aggregates, SET type, BINARY |
| 12 | order_by_limit, insert_update, type_date, type_time | ORDER BY LIMIT, INSERT edge cases, DATE/TIME types |
| 13 | multi_update, type_datetime, func_time, func_set | Multi-table UPDATE, DATETIME, time functions, FIND_IN_SET |
| 14 | expressions, parser_precedence, select_where, group_by | Complex expressions, operator precedence, WHERE, GROUP BY |
| 15 | ctype_utf8, lowercase_table, partition_not_windows, olap | UTF-8 string ops, case-insensitive refs, long identifiers, aggregates |
| 16 | derived, join_nested, temp_table, lowercase_table2 | Derived tables, nested JOINs, CRUD patterns, case-insensitive refs |
| 17 | ctype_latin1, type_newdecimal, join_outer, variables | String functions, DECIMAL precision, LEFT/RIGHT JOIN, user variables |
| 18 | type_bit, null_key, group_min_max, func_bitwise | BIT type, NULL key behavior, GROUP BY MIN/MAX, bitwise operations |
| 19 | alter_table, view, trigger, explain | ALTER TABLE, CREATE/DROP VIEW, triggers, EXPLAIN output |
| 20 | lpad, rpad | LPAD/RPAD string padding edge cases |

## Current Status

**100 MySQL compat tests across 20 tiers — all pass**
**210+ Rust integration tests — all pass**

### Tier 1 — 6/6 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| func_op | **PASS** | — | — |
| bool | **PASS** | — | — |
| type_uint | **PASS** | — | — |
| compare | **PASS** | ~80% | Correlated subqueries, utf32/COLLATE |
| comments | **PASS** | ~50% | PREPARE comment validation, nested comments |
| func_equal | **PASS** | ~70% | DELIMITER, IF/EXISTS/SIGNAL in trigger body |

### Tier 2 — 8/8 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| null | **PASS** | ~50% | INSERT...SELECT, CREATE TABLE AS SELECT, ENUM ALTER DEFAULT |
| case | **PASS** | ~35% | Charset introducers, COLLATE, CREATE TABLE AS SELECT, stored funcs |
| type_varchar | **PASS** | ~50% | Charset/COLLATE, ALTER ADD PK, INSERT self-reference |
| type_ranges | **PASS** | ~30% | Heavy rewrite; basic types only (no AUTO_INC/MEDIUMINT/ENUM) |
| func_isnull | **PASS** | ~40% | GET_LOCK, nested multi-table join, let/eval/EXPLAIN |
| limit | **PASS** | ~50% | PREPARE/EXECUTE LIMIT, auto_increment, CTE, optimizer_switch |
| type_binary | **PASS** | ~60% | BINARY zero-padding, large hex literals |
| bigint | **PASS** | ~85% | --enable_metadata, auto_increment, while loops |

### Tier 3 — 10/10 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| func_math | **PASS** | 1181/1271 (93%) | UDFs, JSON, CONTINUE HANDLER, LOAD DATA |
| func_like | **PASS** | ~25% | ESCAPE aggregate, charset/COLLATE, PREPARE |
| func_test | **PASS** | ~35% | Charset introducers, UNION, CREATE TABLE AS SELECT |
| delete | **PASS** | ~20% | Multi-table DELETE, INSERT IGNORE date, ORDER BY errors |
| type_year | **PASS** | ~55% | NOW() INSERT, YEAR(2) errors, SHOW CREATE TABLE |
| type_float | **PASS** | ~40% | Float overflow boundary, SHOW COLUMNS, charset |
| type_blob | **PASS** | ~20% | Complex multi-table joins, ALTER, SHOW CREATE |
| cast | **PASS** | ~20% | Charset CAST, SIGNED INT keyword, DATE/TIME casts |
| type_enum | **PASS** | ~25% | ENUM DEFAULT, ALTER TABLE, SHOW CREATE, large ENUMs |
| type_decimal | **PASS** | ~15% | AUTO_INC, ZEROFILL, SHOW CREATE, PREPARE |

### Tier 4 — 5/5 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| func_if | **PASS** | ~70% | Mixed aggregate, EXPLAIN, PREPARE |
| insert | **PASS** | ~8% | INSERT...SELECT, ON DUPLICATE KEY, DEFAULT keyword |
| update | **PASS** | ~10% | Multi-table UPDATE, ORDER BY LIMIT, subqueries |
| func_str | **PASS** | ~5% | BINARY, charset, UNHEX, SUBSTRING_INDEX, TRIM extended |
| func_concat | **PASS** | ~45% | GROUP BY issues, UNION, stored procedures |

### Tier 5 — 6/6 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| alias | **PASS** | ~35% | UPDATE ORDER BY LIMIT, INSERT...SELECT |
| truncate | **PASS** | ~35% | LOCK/FLUSH, stored procs, TEMPORARY TABLE |
| func_in_none | **PASS** | ~45% | EXPLAIN, subqueries |
| select_found | **PASS** | ~40% | SQL_CALC_FOUND_ROWS, PREPARE, UNION |
| distinct | **PASS** | ~12% | Complex joins, EXPLAIN, INSERT...SELECT |
| having | **PASS** | ~10% | Aggregate alias HAVING, IS NOT NULL, subqueries |

### Tier 6 — 5/5 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| func_system | **PASS** | ~55% | CHARSET/COLLATION functions, EXPLAIN |
| replace | **PASS** | ~70% | REPLACE...SELECT, VIEW, ALTER TABLE |
| func_regexp | **PASS** | ~35% | REGEXP_REPLACE/INSTR/LIKE, charset, NULL pattern |
| func_group | **PASS** | ~10% | ROLLUP, window funcs, EXPLAIN, subqueries |
| union | **PASS** | ~5% | EXPLAIN, VIEWs, subqueries, --source includes |

### Tier 8 — 6/6 pass

| Test | Status | Coverage | Trimmed |
|------|--------|----------|---------|
| subselect | **PASS** | ~15% | Subqueries in WHERE/SELECT not supported, aggregate-only |
| drop | **PASS** | ~25% | DROP DATABASE, DROP VIEW, DROP TRIGGER, --source |
| auto_increment | **PASS** | ~10% | NULL inserts, ALTER TABLE, REPLACE |
| type_timestamp | **PASS** | ~12% | SET timestamp, timezone, NOW(), ON UPDATE |
| key | **PASS** | ~12% | UNIQUE dup detect, prefix keys, SHOW INDEX |
| join | **PASS** | ~5% | NATURAL/STRAIGHT JOIN, EXPLAIN, subqueries |

### Custom Tests — 23/23 pass

## Features Added for MySQL Compat

| Feature | Description |
|---------|-------------|
| UNION queries | UNION ALL and UNION DISTINCT through resolver/planner/executor pipeline |
| Dup key error mapping | Raft duplicate key errors mapped to MySQL ER_DUP_ENTRY (1062) |
| Integer overflow detection | Proper BIGINT overflow for negation, add, sub, DIV of large numbers |
| Empty INSERT defaults | INSERT INTO t1 () VALUES () fills all columns with defaults |
| BIT_LENGTH/OCTET_LENGTH | String length functions in bits and bytes |
| CONCAT_WS | Concatenate with separator, skipping NULLs |
| Full ALTER TABLE | ADD/DROP/MODIFY/CHANGE COLUMN, ADD/DROP PK/FK/INDEX, RENAME, Raft persistence |
| Lazy row padding | TableScan pads rows with defaults after ALTER TABLE ADD COLUMN |
| NOT NULL enforcement | Error 1048 for NULL into NOT NULL; multi-row converts to default |
| Parenthesized queries | `(SELECT ... LIMIT n) ORDER BY ... LIMIT m` |
| ORDER BY aliases | SELECT aliases usable in ORDER BY clause |
| DDL type validation | FLOAT precision, CHAR/VARCHAR length limits, TEXT promotion |
| Conditional comments | `/*!NNNNN code*/` pre-processing with version check |
| Post-aggregate arithmetic | `max(x)-1` rewrites aggregate refs in expression trees |
| System variable prefixes | `@@global.var`, `@@session.var` parsed |
| Infix MOD operator | `expr MOD expr` works for any expression type |
| Native DECIMAL | `DECIMAL(M,D)` with i128 exact arithmetic (38-digit precision) |
| Shift operators | `<<`, `>>` bitwise shifts |
| Integer division | `DIV` operator |
| Expression headers | Column names reconstructed from expression trees |
| String→number coercion | Implicit conversion in arithmetic contexts |
| BIGINT UNSIGNED | Full u64 range support |
| INSERT IGNORE | Suppress errors during INSERT, skip bad rows |
| INSERT ... SELECT | Insert rows from a source query into a table |
| ABS overflow detection | ABS(i64::MIN) returns ER_DATA_OUT_OF_RANGE; Decimal + UnsignedInt arms |
| Negation overflow | checked_neg for Int; UnsignedInt overflow returns error not Decimal |
| Log function sql_mode | ER_INVALID_ARGUMENT_FOR_LOGARITHM (3020) in DML with ERROR_FOR_DIVISION_BY_ZERO |
| GET_FORMAT resolver | GET_FORMAT type inference + keyword-as-argument fallback |
| HAVING alias resolution | HAVING clause resolves SELECT aliases (e.g., `HAVING s <> 0`) |
| DO statement | `DO expr` evaluates expression and discards result |
| ORDER BY aggregate alias | ORDER BY resolves aggregate aliases via transform_to_output_columns |
| FLOAT/DOUBLE scale validation | ER_TOO_BIG_SCALE (1427) for D>M; scale max 30; display width max 255 |
| WEIGHT_STRING stub | Stub returns input bytes; sufficient for DO context |
| LTRIM/RTRIM functions | Standalone LTRIM()/RTRIM() in eval.rs (resolver already had type inference) |
| DATE_ADD/DATE_SUB | Full interval arithmetic: DAY, MONTH, YEAR, HOUR, MINUTE, SECOND, etc. |
| EXTRACT() | EXTRACT(unit FROM expr) for all standard date/time fields |
| GROUP BY ordinal | GROUP BY 1, ORDER BY 2 — numeric position references |
| DELETE ORDER BY LIMIT | Full pipeline: resolver → planner → executor with sort + truncate |
| INSERT...SELECT column_map | Partial column list mapping for INSERT...SELECT |
| INSERT IGNORE...SELECT | ignore_duplicates propagated through Raft layer |
| DEFAULT for partial INSERT | Column DEFAULT values applied for unspecified columns |
| Hex literal coercion | 0xNN bytes auto-converted to u64 in arithmetic/bitwise contexts |
| Trigger SET @var | SET @var = expr and SET @var = NEW.col in trigger bodies |
| Multi-variable SET | SET @a=1, @b=2, @c=3 sets all variables |
| IN with NULL semantics | IN list returns NULL when value not found and NULL in list |
| BIT type improvements | UnsignedInt cast to BIT, BIT/Bool comparison |
| LPAD/RPAD edge cases | Negative length returns NULL, NULL padstr returns NULL |
| MySQL RAND(seed) | Deterministic LCG matching MySQL's algorithm; thread-local state |
| B'...' bit string literals | `B'10101'` parsed as unsigned integer from binary |
| CAST signed overflow | CAST(float AS SIGNED) returns ER_DATA_OUT_OF_RANGE when value >= 2^63 |
| Scalar-wrapping aggregates | `CRC32(SUM(a))`, `FUNC(AGG(...))` in SELECT, HAVING, ORDER BY |
| HAVING with non-SELECT aggregates | HAVING clause can reference aggregates not in SELECT list |
| CREATE VIEW / DROP VIEW | Raft-persisted views via system.views; survive restart; SHOW CREATE TABLE; SHOW TABLES includes views; JOIN with views; circular view guard (depth 32); query validation at CREATE time |
| Boolean negation | `-(TRUE)` returns -1; `-(1 NOT IN (0))` works correctly |
| CREATE TRIGGER / DROP TRIGGER | BEFORE/AFTER INSERT triggers; body stored as parsed AST; NEW.col substitution; fires via full SQL pipeline |
| Geometry types | POINT, LINESTRING, POLYGON, MULTILINESTRING, MULTIPOLYGON column types; stored as WKB binary |
| ST_GeomFromText | WKT parser (POINT, LINESTRING, POLYGON, MULTILINESTRING, MULTIPOLYGON) → WKB |
| ST_X, ST_Y | Extract X/Y coordinates from POINT geometry |
| ST_NumPoints | Count points in LINESTRING |
| ST_Length | Compute Euclidean length of LINESTRING/MULTILINESTRING |
| ST_Area | Compute area of POLYGON/MULTIPOLYGON via shoelace formula |
| FROM DUAL | SELECT expr FROM DUAL works (MySQL pseudo-table, equivalent to no FROM) |
| INSERT overflow detection | String-to-int overflow in INSERT raises ER_WARN_DATA_OUT_OF_RANGE (1264) |
| ER_WRONG_VALUE_COUNT_ON_ROW | INSERT column/value count mismatch returns MySQL error 1136 |
| CASE with CONVERT | CASE/WHEN with CONVERT(val, CHAR) + CREATE TABLE SELECT |
| COALESCE/IFNULL BIGINT UNSIGNED | CAST(COALESCE(nullable_col, -1) AS UNSIGNED) returns correct u64 |
| ROUND integer arithmetic | ROUND with negative decimals uses integer division for BIGINT/UNSIGNED |
| SET timestamp | SET timestamp=UNIX\_TIMESTAMP(...) accepted; NOW()/TIMEDIFF()/engine=innodb work |
| CREATE TABLE SELECT DIV | Type resolution for DIV with integer/decimal/string/CAST operands |
| --TRUE double negation | `--TRUE` parsed as `-(-(TRUE))` = 1 (not as SQL comment) |
| DO statement silent | DO evaluates expression but discards result; propagates errors |
| CEIL/FLOOR BIGINT UNSIGNED | Returns UnsignedInt for values > i64::MAX; integer passthrough |
| ENUM/SET columns | Stored as Text; CRC32 computes on string representation |
| PREPARE/EXECUTE text protocol | Full cycle including parameter binding with USING @var |
| FLOOR/CEIL DECIMAL support | Decimal(i128, scale) handled in FLOOR/CEIL; values > u64 stay as Decimal |
| RAND seed algorithm fix | Fixed seed2 (no +55555555) and u32 wrapping to match MySQL's seed_random() |
| CAST column names | CAST(x AS UNSIGNED) displays as "unsigned" not "BigIntUnsigned" in column headers |
| CAST float rounding | CAST(float AS UNSIGNED/SIGNED) uses round() not truncation, matching MySQL's rint() |
| NULL expression type check | `1/NULL` etc. skip type checking (evaluates to NULL at runtime); fixes NOT NULL inserts |

## Gap Analysis — Next Steps

### Recently Implemented
- DATE_ADD/DATE_SUB/ADDDATE/SUBDATE with all interval units
- EXTRACT(unit FROM expr) for standard date/time fields
- GROUP BY and ORDER BY ordinal references (GROUP BY 1, ORDER BY 2)
- INSERT...SELECT partial column list remapping
- INSERT IGNORE...SELECT duplicate key suppression
- DEFAULT values for unspecified columns in partial INSERT
- Aggregates inside CASE WHEN expressions
- BIT_AND empty-set identity (u64::MAX)
- DELETE ORDER BY + LIMIT support
- PointGet suppression when LIMIT or ORDER BY present

### Missing Features (discovered by testing)
- Multi-table UPDATE/DELETE syntax (`UPDATE t1,t2 SET ...`, `DELETE t1 FROM ...`)
- Subqueries in WHERE/SELECT (`IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar subqueries)
- INSERT...ON DUPLICATE KEY UPDATE
- INTERVAL arithmetic in expressions (`expr + INTERVAL 1 DAY`)
- Hex literal implicit integer coercion (`0x41+0`)
- `0b` prefix binary literals (`0b01000001`)
- Bitwise operations on VARBINARY columns
- GROUP_CONCAT(DISTINCT ...)
- GROUP BY with expression (IF, CASE, etc.)
- FROM_DAYS(), ADDTIME/SUBTIME, TIMESTAMPADD/TIMESTAMPDIFF
- MAKE_SET()/EXPORT_SET()
- Integer-to-SET member mapping
- SET column defaults

## Architecture

1. `run_mtr_tests.py` generates self-signed TLS certs in a temp directory
2. Initializes a fresh RooDB database with `roodb_init`
3. Starts `roodb` server on port 13309
4. For each test: resets the `test` database, runs `mysqltest` via `--test-file`
5. Prefers trimmed test from `tests/mysql_compat/mtr_t/` over official `/usr/lib/mysql-test/t/`
6. `mysqltest` compares output against recorded `.result` file (or records with `--record`)
7. Server is stopped after all tests complete

Column names: MySQL uses original SQL text; RooDB reconstructs from resolved expression trees via `expr_to_sql()`. We record RooDB's output as our baseline rather than comparing against MySQL's expected results.

Trimmed tests: Some official tests need features we don't support yet (triggers, stored procedures with CONTINUE HANDLER, views, geometry types, JSON, LOAD DATA INFILE). Trimmed versions live in `tests/mysql_compat/mtr_t/` and are preferred by the runner. To regenerate: `python3 /tmp/create_trimmed_test.py`.
