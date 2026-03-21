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
| INSERT IGNORE | type_uint, compare | MySQL extension for ignoring constraint violations |
| CREATE TRIGGER | func_equal | Trigger DDL not supported |
| --error directive matching | comments | Error code mismatch (1064 vs 1105) |
| NOT on nullable columns | bool | `ifnull(not A, 'N')` type issues |
| Hex literal INSERT | compare | `INSERT INTO t1 VALUES (0x01)` |

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
