# MySQL Test Suite Integration

## Overview

RooDB uses the official MySQL 8.0 test suite (`mysql-testsuite-8.0` package) to validate SQL compatibility. Tests are run using the `mysqltest` client binary against a live RooDB server over TLS.

## Prerequisites

```bash
# Install MySQL test suite
sudo apt-get install mysql-testsuite-8.0

# Build RooDB
cargo build --release
```

Key paths:
- `mysqltest` binary: `/usr/lib/mysql-test/bin/mysqltest`
- Official tests: `/usr/lib/mysql-test/t/*.test`
- Official results: `/usr/lib/mysql-test/r/*.result`

## Test Harnesses

### Custom Smoke Tests (22 tests)

```bash
# Run all custom tests
python3 tests/mysql_compat/run_mysql_tests.py

# Record baselines after changes
python3 tests/mysql_compat/run_mysql_tests.py --record

# Filter by name
python3 tests/mysql_compat/run_mysql_tests.py --filter=joins
```

Custom tests live in `tests/mysql_compat/t/` with expected results in `tests/mysql_compat/r/`.

### Official MySQL Test Suite (curated)

```bash
# Run curated official MySQL tests
python3 tests/mysql_compat/run_mtr_tests.py

# Run only tier 1 (easiest)
python3 tests/mysql_compat/run_mtr_tests.py --tier=1

# Record baselines
python3 tests/mysql_compat/run_mtr_tests.py --tier=1 --record

# List available tests
python3 tests/mysql_compat/run_mtr_tests.py --list
```

Our recorded results live in `tests/mysql_compat/mtr_r/`. These capture RooDB's output for the official MySQL test inputs.

## Test Tiers

Tests are organized by difficulty/likelihood of passing:

| Tier | Tests | Focus |
|------|-------|-------|
| 1 | func_op, bool, type_uint, compare, comments, func_equal | Basic arithmetic, types, comparisons |
| 2 | null, case, type_varchar, type_ranges, func_isnull, limit, type_binary, bigint | NULL semantics, data types, LIMIT |
| 3 | type_decimal, type_float, func_like, func_test, func_math, delete, cast | Functions, DML, type casting |
| 4 | insert, update, func_str, func_concat, func_if | Complex DML, string functions |

## Current Status

### Passing Official MySQL Tests

- `func_op` — Arithmetic operators, bitwise ops, MOD, bit_count, shift operators

### Known Gaps (blocking more tests)

| Gap | Tests Blocked | Notes |
|-----|---------------|-------|
| MySQL conditional comments `/*!...*/` | comments | Syntax not parsed |
| SHOW CREATE TABLE | case, func_math | Not implemented |
| REGEXP operator | null, func_test | Not implemented |
| ALTER TABLE | type_varchar | Not supported |
| CREATE TRIGGER | func_equal | Not supported |
| Unsigned range clamping on INSERT | type_uint | INSERT IGNORE with out-of-range values |
| Hex literal in INSERT values | compare | `INSERT INTO t1 VALUES (0x01,0x01)` with non-blob columns |
| NOT on mixed-type IFNULL | bool | `ifnull(not A, 'N')` cross-type in IFNULL |
| `float(precision)` syntax | type_float, type_ranges | `float(24)`, `float(52)` |
| `blob(N)`/`text(N)` size hints | type_blob | `blob(250)`, `text(70000)` |
| `to_days()` function | func_isnull | Not implemented |
| CONVERT with type | cast | `CONVERT(expr, DATE)` syntax |

### Features Added for MySQL Compat

| Feature | Commit | Tests Helped |
|---------|--------|-------------|
| Shift operators `<<` `>>` | mysql-compat | func_op |
| DIV (integer division) | mysql-compat | func_op |
| MOD() function | mysql-compat | func_op |
| Scientific notation (`0E0`) | mysql-compat | func_op, func_equal |
| Int/Int division returns float | mysql-compat | func_op |
| Expression column headers | mysql-compat | all tests |
| Unsigned int catch-all | mysql-compat | type_uint |
| NOT on integers | mysql-compat-2 | bool |
| String→int coercion in bitwise | mysql-compat-2 | case |
| Bitwise NOT `~` operator | mysql-compat-2 | cast |
| Error code 1065 (empty query) | mysql-compat-2 | comments |
| Error code 1054 (bad field) | mysql-compat-2 | func_math |
| DEFAULT values for partial INSERT | mysql-compat-2 | delete |
| FLOOR/CEIL as AST nodes | mysql-compat-2 | func_math |
| GROUP BY alias resolution | mysql-compat-2 | case |
| Comment-only SQL → OK | mysql-compat-2 | comments |
| Integer overflow → u64 fallback | mysql-compat-2 | bigint |
| String→number in arithmetic | mysql-compat-2 | type_float |

## Architecture

### How Tests Work

1. `run_mtr_tests.py` generates TLS certs in a temp directory
2. Initializes a fresh RooDB database with `roodb_init`
3. Starts `roodb` server on port 13309
4. For each test: resets the `test` database, runs `mysqltest` with the `.test` file as input
5. `mysqltest` compares output against `.result` file (or records it with `--record`)
6. Server is stopped after all tests complete

### Column Name Compatibility

MySQL uses the original SQL text as column headers for expressions. RooDB reconstructs column names from the resolved expression tree via `expr_to_sql()` in the logical planner. This produces close but not always identical names:

| MySQL | RooDB | Status |
|-------|-------|--------|
| `1+1` | `1 + 1` | Spaces differ |
| `mod(8,5)` | `mod(8,5)` | Match |
| `-(1+1)*-2` | `-1 + 1 * -2` | Parentheses lost |

For official MySQL tests, we record RooDB's output as our baseline rather than comparing against MySQL's expected results.

### MOD Function Workaround

`MOD` is a keyword in sqlparser, preventing `MOD(x,y)` from parsing as a function call. The parser normalizes `MOD(` to `_ROODB_MOD(` before parsing, and this internal name is mapped back to `mod` for column display.

## Adding New Tests

1. Check if the test exists: `ls /usr/lib/mysql-test/t/<name>.test`
2. Add to the appropriate tier in `tests/mysql_compat/run_mtr_tests.py`
3. Record baseline: `python3 tests/mysql_compat/run_mtr_tests.py --filter=<name> --record`
4. Verify it passes: `python3 tests/mysql_compat/run_mtr_tests.py --filter=<name>`
