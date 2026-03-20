#!/usr/bin/env python3
"""
MySQL compatibility test harness for RooDB.

Manages RooDB server lifecycle and runs mysqltest against it.

Usage:
    python3 tests/mysql_compat/run_mysql_tests.py              # run custom smoke tests
    python3 tests/mysql_compat/run_mysql_tests.py --mysql-tests # run MySQL suite tests
    python3 tests/mysql_compat/run_mysql_tests.py --record      # record expected results
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

PORT = 13308  # Use different port from integration tests
HOST = "127.0.0.1"
USER = "root"
PASSWORD = ""


class TlsCerts:
    """Generate TLS certificates for RooDB server."""

    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.ca_cert = os.path.join(tmpdir, "ca.crt")
        self.ca_key = os.path.join(tmpdir, "ca.key")
        self.server_cert = os.path.join(tmpdir, "server.crt")
        self.server_key = os.path.join(tmpdir, "server.key")

    def generate(self):
        """Generate CA + server certificates."""
        # Generate CA key and cert
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", self.ca_key, "-out", self.ca_cert,
                "-days", "1", "-nodes",
                "-subj", "/CN=RooDB Test CA",
            ],
            check=True, capture_output=True,
        )

        # Generate server key
        server_csr = os.path.join(self.tmpdir, "server.csr")
        subprocess.run(
            [
                "openssl", "req", "-newkey", "rsa:2048",
                "-keyout", self.server_key, "-out", server_csr,
                "-nodes",
                "-subj", "/CN=localhost",
            ],
            check=True, capture_output=True,
        )

        # Create SAN extension file
        ext_file = os.path.join(self.tmpdir, "ext.cnf")
        with open(ext_file, "w") as f:
            f.write(
                "[v3_ext]\n"
                "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
                "basicConstraints=CA:FALSE\n"
            )

        # Sign server cert with CA
        subprocess.run(
            [
                "openssl", "x509", "-req",
                "-in", server_csr, "-CA", self.ca_cert, "-CAkey", self.ca_key,
                "-CAcreateserial", "-out", self.server_cert,
                "-days", "1",
                "-extfile", ext_file, "-extensions", "v3_ext",
            ],
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

        # Find binaries from cargo build
        self.roodb_bin = os.path.join(PROJECT_ROOT, "target", "release", "roodb")
        self.roodb_init_bin = os.path.join(PROJECT_ROOT, "target", "release", "roodb_init")

        if not os.path.exists(self.roodb_bin):
            raise RuntimeError(
                f"roodb binary not found at {self.roodb_bin}. Run 'cargo build --release' first."
            )
        if not os.path.exists(self.roodb_init_bin):
            raise RuntimeError(
                f"roodb_init binary not found at {self.roodb_init_bin}. Run 'cargo build --release' first."
            )

    def init(self):
        """Initialize database directory."""
        os.makedirs(self.data_dir, exist_ok=True)
        env = os.environ.copy()
        env["ROODB_ROOT_PASSWORD"] = PASSWORD
        result = subprocess.run(
            [self.roodb_init_bin, "--data-dir", self.data_dir],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"roodb_init failed (rc={result.returncode}):\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )

    def start(self):
        """Start the RooDB server."""
        cmd = [
            self.roodb_bin,
            "--port", str(self.port),
            "--data-dir", self.data_dir,
            "--cert-path", self.certs.server_cert,
            "--key-path", self.certs.server_key,
            "--raft-ca-cert-path", self.certs.ca_cert,
        ]
        self.process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self._wait_ready()

    def _wait_ready(self, timeout=15):
        """Poll until server accepts connections."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                sock = socket.create_connection((HOST, self.port), timeout=1)
                sock.close()
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(0.2)
        raise RuntimeError(f"RooDB server did not become ready within {timeout}s")

    def stop(self):
        """Stop the server."""
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None


class TestResult:
    PASS = "PASS"
    RESULT_MISMATCH = "RESULT_MISMATCH"
    SQL_ERROR = "SQL_ERROR"
    CONNECT_FAIL = "CONNECT_FAIL"
    RECORD = "RECORD"


def run_mysqltest(test_file, result_file, certs, port=PORT, record=False):
    """
    Run a single mysqltest test file against RooDB.

    Returns (status, details) tuple.
    """
    if not os.path.exists(MYSQLTEST_BIN):
        return (TestResult.CONNECT_FAIL, f"mysqltest not found at {MYSQLTEST_BIN}")

    # Reset the test database before each test for isolation
    reset_cmd = [
        MYSQLTEST_BIN,
        f"--host={HOST}",
        f"--port={port}",
        f"--user={USER}",
        f"--password={PASSWORD}",
        "--ssl-mode=REQUIRED",
        f"--ssl-ca={certs.ca_cert}",
    ]
    subprocess.run(
        reset_cmd,
        input="DROP DATABASE IF EXISTS test;\nCREATE DATABASE test;\n",
        capture_output=True,
        text=True,
        timeout=5,
    )

    cmd = [
        MYSQLTEST_BIN,
        f"--host={HOST}",
        f"--port={port}",
        f"--user={USER}",
        f"--password={PASSWORD}",
        "--ssl-mode=REQUIRED",
        f"--ssl-ca={certs.ca_cert}",
        "--database=test",
    ]

    if record:
        cmd.append("--record")

    cmd.append(f"--basedir={MYSQL_TEST_DIR}")
    cmd.append(f"--result-file={result_file}")

    try:
        result = subprocess.run(
            cmd,
            stdin=open(test_file, "r"),
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return (TestResult.SQL_ERROR, "Test timed out after 30s")
    except Exception as e:
        return (TestResult.CONNECT_FAIL, str(e))

    if result.returncode == 0:
        if record:
            return (TestResult.RECORD, f"Recorded to {result_file}")
        return (TestResult.PASS, "")
    elif result.returncode == 1:
        # Result content mismatch
        detail = result.stderr.strip() or result.stdout.strip()
        return (TestResult.RESULT_MISMATCH, detail[:500])
    elif result.returncode == 2:
        # SQL error / connection error
        detail = result.stderr.strip() or result.stdout.strip()
        if "connect" in detail.lower() or "connection" in detail.lower():
            return (TestResult.CONNECT_FAIL, detail[:500])
        return (TestResult.SQL_ERROR, detail[:500])
    else:
        detail = result.stderr.strip() or result.stdout.strip()
        return (TestResult.SQL_ERROR, f"exit code {result.returncode}: {detail[:500]}")


def find_custom_tests():
    """Find all .test files in t/ directory."""
    test_dir = os.path.join(SCRIPT_DIR, "t")
    tests = sorted(glob.glob(os.path.join(test_dir, "*.test")))
    return tests


def find_mysql_suite_tests():
    """Find MySQL test suite .test files."""
    suite_dir = os.path.join(MYSQL_TEST_DIR, "suite", "main", "t")
    if not os.path.exists(suite_dir):
        suite_dir = os.path.join(MYSQL_TEST_DIR, "t")
    if not os.path.exists(suite_dir):
        return []
    return sorted(glob.glob(os.path.join(suite_dir, "*.test")))


def main():
    parser = argparse.ArgumentParser(description="MySQL compatibility test harness for RooDB")
    parser.add_argument(
        "--mysql-tests", action="store_true",
        help="Run MySQL test suite tests instead of custom tests",
    )
    parser.add_argument(
        "--record", action="store_true",
        help="Record expected results (first run)",
    )
    parser.add_argument(
        "--filter", type=str, default="",
        help="Only run tests matching this substring",
    )
    parser.add_argument(
        "--port", type=int, default=PORT,
        help=f"Port for RooDB server (default: {PORT})",
    )
    args = parser.parse_args()

    # Create temp directory for certs and data
    with tempfile.TemporaryDirectory(prefix="roodb_mysql_compat_") as tmpdir:
        print(f"Working directory: {tmpdir}")

        # Generate TLS certs
        certs = TlsCerts(tmpdir)
        print("Generating TLS certificates...")
        certs.generate()

        # Initialize and start RooDB
        server = RooDbServer(tmpdir, certs, port=args.port)
        print("Initializing database...")
        server.init()
        print(f"Starting RooDB on port {args.port}...")
        server.start()

        try:
            # Gather tests
            if args.mysql_tests:
                tests = find_mysql_suite_tests()
                if args.record:
                    # Record mode: write results to tmpdir
                    result_dir = os.path.join(tmpdir, "mysql_results")
                    os.makedirs(result_dir, exist_ok=True)
                else:
                    # Compare mode: use MySQL's own result files
                    result_dir = os.path.join(MYSQL_TEST_DIR, "r")
                    if not os.path.exists(result_dir):
                        result_dir = os.path.join(tmpdir, "mysql_results")
                        os.makedirs(result_dir, exist_ok=True)
            else:
                tests = find_custom_tests()
                result_dir = os.path.join(SCRIPT_DIR, "r")

            if args.filter:
                tests = [t for t in tests if args.filter in os.path.basename(t)]

            if not tests:
                print("No test files found!")
                return 1

            print(f"\nRunning {len(tests)} tests...\n")

            # Run tests and collect results
            results = {
                TestResult.PASS: [],
                TestResult.RESULT_MISMATCH: [],
                TestResult.SQL_ERROR: [],
                TestResult.CONNECT_FAIL: [],
                TestResult.RECORD: [],
            }

            for test_file in tests:
                test_name = os.path.splitext(os.path.basename(test_file))[0]

                # Determine result file path
                if args.mysql_tests:
                    result_file = os.path.join(result_dir, f"{test_name}.result")
                else:
                    result_file = os.path.join(result_dir, f"{test_name}.result")

                status, detail = run_mysqltest(
                    test_file, result_file, certs,
                    port=args.port, record=args.record,
                )

                results[status].append((test_name, detail))

                # Print status
                symbol = {
                    TestResult.PASS: "\033[32m[PASS]\033[0m",
                    TestResult.RESULT_MISMATCH: "\033[33m[MISMATCH]\033[0m",
                    TestResult.SQL_ERROR: "\033[31m[ERROR]\033[0m",
                    TestResult.CONNECT_FAIL: "\033[31m[CONNECT]\033[0m",
                    TestResult.RECORD: "\033[36m[RECORD]\033[0m",
                }[status]
                print(f"  {symbol} {test_name}")
                if detail and status != TestResult.PASS:
                    # Print first line of detail
                    first_line = detail.split("\n")[0][:100]
                    print(f"         {first_line}")

            # Summary
            total = len(tests)
            passed = len(results[TestResult.PASS])
            recorded = len(results[TestResult.RECORD])
            mismatched = len(results[TestResult.RESULT_MISMATCH])
            errors = len(results[TestResult.SQL_ERROR])
            connect_fails = len(results[TestResult.CONNECT_FAIL])

            print(f"\n{'='*60}")
            print(f"Results: {total} total")
            if recorded:
                print(f"  Recorded:  {recorded}")
            print(f"  Passed:    {passed}")
            print(f"  Mismatch:  {mismatched}")
            print(f"  SQL Error: {errors}")
            print(f"  Connect:   {connect_fails}")
            print(f"{'='*60}")

            return 0 if (errors == 0 and connect_fails == 0 and mismatched == 0) else 1

        finally:
            print("\nStopping RooDB server...")
            server.stop()


if __name__ == "__main__":
    sys.exit(main())
