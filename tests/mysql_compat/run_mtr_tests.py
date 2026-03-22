#!/usr/bin/env python3
"""
Official MySQL Test Suite runner for RooDB.

Runs curated subsets of the official MySQL test suite (/usr/lib/mysql-test/t/)
against RooDB, recording and comparing results.

Usage:
    python3 tests/mysql_compat/run_mtr_tests.py                    # run all curated tests
    python3 tests/mysql_compat/run_mtr_tests.py --record            # record baseline results
    python3 tests/mysql_compat/run_mtr_tests.py --filter=func_op    # run matching tests
    python3 tests/mysql_compat/run_mtr_tests.py --tier=1            # run only tier 1 (easiest)
    python3 tests/mysql_compat/run_mtr_tests.py --tier=1,2          # tiers 1 and 2
    python3 tests/mysql_compat/run_mtr_tests.py --list              # list tests without running
"""

import argparse
import glob
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
MYSQLTEST_BIN = "/usr/lib/mysql-test/bin/mysqltest"
MYSQL_TEST_DIR = "/usr/lib/mysql-test"
MYSQL_TEST_T = os.path.join(MYSQL_TEST_DIR, "t")
MYSQL_TEST_R = os.path.join(MYSQL_TEST_DIR, "r")

# Our recorded results for official MySQL tests
MTR_RESULTS_DIR = os.path.join(SCRIPT_DIR, "mtr_r")

PORT = 13309
HOST = "127.0.0.1"
USER = "root"
PASSWORD = ""

# ---------------------------------------------------------------------------
# Test tiers — curated lists of official MySQL tests ranked by difficulty.
#
# Tier 1: Tiny, self-contained, basic SQL (most likely to pass)
# Tier 2: Moderate complexity, standard SQL features
# Tier 3: Larger tests, more MySQL features needed
# Tier 4: Complex tests requiring many MySQL features
# ---------------------------------------------------------------------------

TIERS = {
    1: [
        "func_op",         # 36 lines — arithmetic, bit ops
        "bool",            # 62 lines — boolean/NULL logic
        "type_uint",       # 18 lines — unsigned int basics
        "compare",         # 155 lines — comparisons, BETWEEN, IN, LIKE
        "comments",        # 103 lines — SQL comment syntax
        "func_equal",      # 73 lines — <=> null-safe equal
    ],
    2: [
        "null",            # 324 lines — NULL semantics
        "case",            # 396 lines — CASE/WHEN
        "type_varchar",    # 176 lines — VARCHAR behavior
        "type_ranges",     # 173 lines — all basic data types
        "func_isnull",     # 170 lines — ISNULL() function
        "limit",           # 448 lines — LIMIT/OFFSET
        "type_binary",     # 198 lines — binary/varbinary
        "bigint",          # 502 lines — bigint arithmetic
    ],
    3: [
        "type_decimal",    # 617 lines — decimal precision
        "type_float",      # 504 lines — float precision
        "func_like",       # 396 lines — LIKE operator
        "func_test",       # 483 lines — comparison functions
        "func_math",       # 1271 lines — math functions
        "delete",          # 1026 lines — DELETE variants
        "cast",            # 1148 lines — CAST/CONVERT
        "type_year",       # year type
        "type_blob",       # blob handling
        "type_enum",       # enum type
    ],
    4: [
        "insert",          # 1077 lines — INSERT variants (has --source)
        "update",          # 780 lines — UPDATE variants (has --source)
        "func_str",        # 2630 lines — string functions (many includes)
        "func_concat",     # 153 lines — CONCAT (has includes)
        "func_if",         # 301 lines — IF function (has includes)
    ],
}


class TlsCerts:
    """Generate TLS certificates for RooDB server."""

    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.ca_cert = os.path.join(tmpdir, "ca.crt")
        self.ca_key = os.path.join(tmpdir, "ca.key")
        self.server_cert = os.path.join(tmpdir, "server.crt")
        self.server_key = os.path.join(tmpdir, "server.key")

    def generate(self):
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048",
             "-keyout", self.ca_key, "-out", self.ca_cert,
             "-days", "1", "-nodes", "-subj", "/CN=RooDB Test CA"],
            check=True, capture_output=True,
        )
        server_csr = os.path.join(self.tmpdir, "server.csr")
        subprocess.run(
            ["openssl", "req", "-newkey", "rsa:2048",
             "-keyout", self.server_key, "-out", server_csr,
             "-nodes", "-subj", "/CN=localhost"],
            check=True, capture_output=True,
        )
        ext_file = os.path.join(self.tmpdir, "ext.cnf")
        with open(ext_file, "w") as f:
            f.write("[v3_ext]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n"
                    "basicConstraints=CA:FALSE\n")
        subprocess.run(
            ["openssl", "x509", "-req", "-in", server_csr,
             "-CA", self.ca_cert, "-CAkey", self.ca_key,
             "-CAcreateserial", "-out", self.server_cert,
             "-days", "1", "-extfile", ext_file, "-extensions", "v3_ext"],
            check=True, capture_output=True,
        )


class RooDbServer:
    """Manages a RooDB server process."""

    def __init__(self, tmpdir, certs, port=PORT):
        self.tmpdir = tmpdir
        self.data_dir = os.path.join(tmpdir, "data")
        self.certs = certs
        self.port = port
        self.process = None
        self.roodb_bin = os.path.join(PROJECT_ROOT, "target", "release", "roodb")
        self.roodb_init_bin = os.path.join(PROJECT_ROOT, "target", "release", "roodb_init")
        for b in [self.roodb_bin, self.roodb_init_bin]:
            if not os.path.exists(b):
                raise RuntimeError(f"{b} not found. Run 'cargo build --release' first.")

    def init(self):
        os.makedirs(self.data_dir, exist_ok=True)
        env = os.environ.copy()
        env["ROODB_ROOT_PASSWORD"] = PASSWORD
        result = subprocess.run(
            [self.roodb_init_bin, "--data-dir", self.data_dir],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"roodb_init failed: {result.stderr}")

    def start(self):
        cmd = [
            self.roodb_bin,
            "--port", str(self.port),
            "--data-dir", self.data_dir,
            "--cert-path", self.certs.server_cert,
            "--key-path", self.certs.server_key,
            "--raft-ca-cert-path", self.certs.ca_cert,
        ]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._wait_ready()

    def _wait_ready(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                sock = socket.create_connection((HOST, self.port), timeout=1)
                sock.close()
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(0.2)
        raise RuntimeError(f"RooDB did not start within {timeout}s")

    def stop(self):
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None


def reset_test_db(certs, port):
    """Reset the test database between tests.

    RooDB doesn't scope tables to databases, so we need to explicitly
    drop all user tables, not just recreate the database.
    """
    cmd = [
        MYSQLTEST_BIN,
        f"--host={HOST}", f"--port={port}", f"--user={USER}",
        f"--password={PASSWORD}", "--ssl-mode=REQUIRED",
        f"--ssl-ca={certs.ca_cert}",
        "--database=test",
    ]
    # First get list of all tables
    result = subprocess.run(
        cmd,
        input="SHOW TABLES;\n",
        capture_output=True, text=True, timeout=10,
    )
    # Parse table names from output (skip header line)
    tables = []
    if result.returncode == 0:
        for line in result.stdout.strip().split('\n'):
            name = line.strip()
            # Skip header, empty lines, system tables, and invalid names
            if (name and not name.startswith('Tables_in_')
                    and not name.startswith('system.')
                    and '`' not in name):
                tables.append(name)

    # Drop all user tables, then recreate the database.
    # Also brute-force drop common test table names (t1-t9) in case SHOW TABLES missed them.
    drop_sql = ""
    for table in tables:
        drop_sql += f"DROP TABLE IF EXISTS `{table}`;\n"
    for i in range(1, 10):
        drop_sql += f"DROP TABLE IF EXISTS t{i};\n"
    drop_sql += "DROP DATABASE IF EXISTS test;\nCREATE DATABASE test;\n"

    subprocess.run(
        cmd[:len(cmd)-1],  # Remove --database=test since we're dropping it
        input=drop_sql,
        capture_output=True, text=True, timeout=10,
    )


def run_one_test(test_name, certs, port, record, log_dir):
    """
    Run a single official MySQL test against RooDB.

    Returns (status, detail) where status is one of:
        'pass', 'mismatch', 'error', 'skip'
    """
    test_file = os.path.join(MYSQL_TEST_T, f"{test_name}.test")
    if not os.path.exists(test_file):
        return ("skip", f"test file not found: {test_file}")

    result_file = os.path.join(MTR_RESULTS_DIR, f"{test_name}.result")

    # Reset test DB
    reset_test_db(certs, port)

    # Write preamble + test content to a temp file inside the mysql-test dir
    # so that --source include/... paths resolve correctly via --basedir.
    preamble = "--let $DEFAULT_ENGINE = RooDB\n"
    with open(test_file, "r") as f:
        test_content = preamble + f.read()
    tmp_test_file = os.path.join(log_dir, f"{test_name}.test")
    with open(tmp_test_file, "w") as f:
        f.write(test_content)

    cmd = [
        MYSQLTEST_BIN,
        f"--host={HOST}", f"--port={port}", f"--user={USER}",
        f"--password={PASSWORD}", "--ssl-mode=REQUIRED",
        f"--ssl-ca={certs.ca_cert}",
        "--database=test",
        f"--basedir={MYSQL_TEST_DIR}/",
        f"--logdir={log_dir}",
        f"--result-file={result_file}",
        f"--test-file={tmp_test_file}",
    ]
    if record:
        cmd.append("--record")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return ("error", "timeout after 60s")
    except Exception as e:
        return ("error", str(e))

    if result.returncode == 0:
        if record:
            return ("recorded", f"→ {result_file}")
        return ("pass", "")
    elif result.returncode == 1:
        detail = result.stderr.strip() or result.stdout.strip()
        return ("mismatch", detail[:800])
    elif result.returncode == 62:
        # mysqltest returns 62 for --source file not found and similar
        detail = result.stderr.strip() or result.stdout.strip()
        return ("error", detail[:800])
    else:
        detail = result.stderr.strip() or result.stdout.strip()
        return ("error", f"exit {result.returncode}: {detail[:800]}")


def main():
    parser = argparse.ArgumentParser(description="Run official MySQL tests against RooDB")
    parser.add_argument("--record", action="store_true", help="Record baseline results")
    parser.add_argument("--filter", type=str, default="", help="Only run tests matching substring")
    parser.add_argument("--tier", type=str, default="", help="Comma-separated tier numbers (e.g. 1,2)")
    parser.add_argument("--list", action="store_true", help="List tests without running")
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()

    # Determine which tiers to run
    if args.tier:
        selected_tiers = [int(t) for t in args.tier.split(",")]
    else:
        selected_tiers = sorted(TIERS.keys())

    # Gather test names
    tests = []
    for tier in selected_tiers:
        for name in TIERS.get(tier, []):
            tests.append((tier, name))

    if args.filter:
        tests = [(t, n) for t, n in tests if args.filter in n]

    if not tests:
        print("No tests selected.")
        return 1

    if args.list:
        for tier, name in tests:
            test_file = os.path.join(MYSQL_TEST_T, f"{name}.test")
            exists = "ok" if os.path.exists(test_file) else "MISSING"
            result_file = os.path.join(MTR_RESULTS_DIR, f"{name}.result")
            has_baseline = "recorded" if os.path.exists(result_file) else "no baseline"
            print(f"  tier {tier}  {name:30s}  [{exists}] [{has_baseline}]")
        print(f"\nTotal: {len(tests)} tests")
        return 0

    # Ensure results directory exists
    os.makedirs(MTR_RESULTS_DIR, exist_ok=True)

    # Check mysqltest binary
    if not os.path.exists(MYSQLTEST_BIN):
        print(f"ERROR: mysqltest not found at {MYSQLTEST_BIN}")
        print("Install: sudo apt-get install mysql-testsuite-8.0")
        return 1

    with tempfile.TemporaryDirectory(prefix="roodb_mtr_") as tmpdir:
        log_dir = os.path.join(tmpdir, "logs")
        os.makedirs(log_dir)

        print("Generating TLS certificates...")
        certs = TlsCerts(tmpdir)
        certs.generate()

        server = RooDbServer(tmpdir, certs, port=args.port)
        print("Initializing database...")
        server.init()
        print(f"Starting RooDB on port {args.port}...")
        server.start()

        try:
            print(f"\nRunning {len(tests)} official MySQL tests...\n")

            counts = {"pass": 0, "mismatch": 0, "error": 0, "skip": 0, "recorded": 0}
            failures = []

            for tier, test_name in tests:
                status, detail = run_one_test(test_name, certs, args.port, args.record, log_dir)
                counts[status] += 1

                symbols = {
                    "pass":     "\033[32m[PASS]\033[0m",
                    "mismatch": "\033[33m[MISMATCH]\033[0m",
                    "error":    "\033[31m[ERROR]\033[0m",
                    "skip":     "\033[90m[SKIP]\033[0m",
                    "recorded": "\033[36m[RECORD]\033[0m",
                }
                print(f"  {symbols[status]}  T{tier} {test_name}")
                if detail and status in ("mismatch", "error"):
                    # Show first meaningful line
                    for line in detail.split("\n"):
                        line = line.strip()
                        if line and not line.startswith("WARNING:"):
                            print(f"           {line[:120]}")
                            break
                    failures.append((tier, test_name, status, detail))

            # Summary
            total = sum(counts.values())
            print(f"\n{'='*60}")
            print(f"Official MySQL Test Results: {total} total")
            if counts["recorded"]:
                print(f"  Recorded:  {counts['recorded']}")
            print(f"  Passed:    {counts['pass']}")
            print(f"  Mismatch:  {counts['mismatch']}")
            print(f"  Error:     {counts['error']}")
            print(f"  Skip:      {counts['skip']}")
            print(f"{'='*60}")

            if failures:
                print(f"\nFailed tests:")
                for tier, name, status, detail in failures:
                    print(f"  T{tier} {name} [{status}]")

            return 0 if counts["error"] == 0 and counts["mismatch"] == 0 else 1

        finally:
            print("\nStopping RooDB server...")
            server.stop()


if __name__ == "__main__":
    sys.exit(main())
