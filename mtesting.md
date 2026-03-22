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
# Custom compatibility tests (22 tests, all pass)
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

## Current Status

### Tier 1 — 3/6 pass

| Test | Status | Blocking Feature |
|------|--------|-----------------|
| func_op | **PASS** | — |
| bool | **PASS** | — |
| type_uint | **PASS** | — |
| compare | FAIL | Correlated scalar subqueries (line 72) |
| comments | FAIL | SQL-level PREPARE stmt + unclosed comment validation |
| func_equal | FAIL | CREATE TRIGGER (line 56) |

### Tier 2 — 0/8 pass (no recorded baselines yet)

| Test | Status | Blocking Feature | Lines Passed |
|------|--------|-----------------|-------------|
| bigint | FAIL | mysqltest WHILE loop control flow | ~350/500 |
| null | FAIL | NOT NULL constraint on expr results | ~70/324 |
| limit | FAIL | Parenthesized query expressions | ~15/448 |
| case | FAIL | Charset introducers (_latin1), COLLATE | ~74/396 |
| type_varchar | FAIL | PAD SPACE collation, ALTER TABLE | ~20/176 |
| type_ranges | FAIL | ENUM, SET, display widths, many types | ~58/173 |
| func_isnull | FAIL | ISNULL(), GET_LOCK(), subqueries | ~25/170 |
| type_binary | FAIL | --replace_result mysqltest directive | ~23/198 |

### Custom Tests — 22/22 pass

## Features Added for MySQL Compat

| Feature | Description |
|---------|-------------|
| Conditional comments | `/*!NNNNN code*/` pre-processing with version check |
| Post-aggregate arithmetic | `max(x)-1` rewrites aggregate refs in expression trees |
| System variable prefixes | `@@global.var`, `@@session.var` parsed; added default_storage_engine |
| Infix MOD operator | `expr MOD expr` works for any expression type |
| Native DECIMAL | `DECIMAL(M,D)` with i128 exact arithmetic (38-digit precision) |
| Shift operators | `<<`, `>>` bitwise shifts |
| Integer division | `DIV` operator |
| Expression headers | Column names reconstructed from expression trees |
| String→number coercion | Implicit conversion in arithmetic contexts |
| BIGINT UNSIGNED | Full u64 range support |

## Gap Analysis

### Quick Fixes
- NOT NULL constraint enforcement on expression results
- ISNULL() function (alias for expr IS NULL)

### Medium Features
- PAD SPACE comparison for varchar
- Parenthesized query expressions `(SELECT ...) ORDER BY`
- SQL-level PREPARE/EXECUTE/DEALLOCATE
- ALTER TABLE (ADD COLUMN, MODIFY, ADD KEY, ADD PRIMARY KEY)

### Large Features
- Scalar/correlated subqueries
- CREATE TRIGGER / trigger execution
- Charset introducers (`_latin1'...'`) and COLLATE
- ENUM and SET types
- REGEXP operator

## Architecture

1. `run_mtr_tests.py` generates self-signed TLS certs in a temp directory
2. Initializes a fresh RooDB database with `roodb_init`
3. Starts `roodb` server on port 13309
4. For each test: resets the `test` database, runs `mysqltest` with the `.test` file
5. `mysqltest` compares output against recorded `.result` file (or records with `--record`)
6. Server is stopped after all tests complete

Column names: MySQL uses original SQL text; RooDB reconstructs from resolved expression trees via `expr_to_sql()`. We record RooDB's output as our baseline rather than comparing against MySQL's expected results.
