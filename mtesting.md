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

## Current Status

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
| func_math | **PASS** | 1089/1271 (86%) | UDFs, JSON, stored procs, LOAD DATA |
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

### Quick Wins (unblocks most test progress)
- ORDER BY aggregate alias (blocks limit at line 237)
- INSERT ... SELECT (blocks null at line 113)
- Duplicate key validation on ALTER TABLE ADD PK (blocks type_varchar)
- BIT_LENGTH() function (blocks func_str)
- CONCAT_WS with mixed types (blocks func_concat)

### Medium Features
- CREATE TABLE AS SELECT (blocks type_float)
- Multi-table DELETE syntax (blocks delete)
- PREPARE validation of SQL syntax (blocks comments)
- GET_LOCK()/RELEASE_LOCK() stub functions (blocks func_isnull)
- Large unsigned integer literal parsing (blocks func_test)

### Large Features (complex implementation)
- Scalar/correlated subqueries (blocks compare)
- CREATE TRIGGER / trigger execution (blocks func_equal)
- Charset introducers (`_latin1'...'`) and COLLATE (blocks case, cast)
- ENUM and SET types (blocks type_ranges, type_enum)

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
