#!/usr/bin/env bash
#
# RooDB vs MySQL sysbench benchmark
#
# Usage: bench/run.sh [OPTIONS]
#   --workload=WORKLOAD    oltp_point_select|oltp_read_only|oltp_read_write|oltp_write_only|all (default: all)
#   --table-size=N         rows per table (default: 10000)
#   --tables=N             number of tables (default: 1)
#   --threads=N            concurrent threads (default: 1)
#   --duration=N           seconds (default: 60)
#   --roodb-only           skip MySQL
#   --mysql-only           skip RooDB
#   --skip-build           skip cargo build
#   --output=FILE          custom JSON output path

set -euo pipefail

# ─── Constants ────────────────────────────────────────────────────────────────
MYSQL_PORT=3316
MYSQL_CONTAINER="roodb-bench-mysql"
MYSQL_IMAGE="mysql:8.0"
ROODB_PORT=3317
BENCH_PASSWORD="benchpass"
BENCH_DB="sbtest"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SYSBENCH_TIMEOUT=300

# ─── Defaults ─────────────────────────────────────────────────────────────────
WORKLOAD="all"
TABLE_SIZE=10000
TABLES=1
THREADS=1
DURATION=60
RUN_MYSQL=true
RUN_ROODB=true
SKIP_BUILD=false
OUTPUT_FILE=""

# ─── Parse args ───────────────────────────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --workload=*)    WORKLOAD="${arg#*=}" ;;
        --table-size=*)  TABLE_SIZE="${arg#*=}" ;;
        --tables=*)      TABLES="${arg#*=}" ;;
        --threads=*)     THREADS="${arg#*=}" ;;
        --duration=*)    DURATION="${arg#*=}" ;;
        --roodb-only)    RUN_MYSQL=false ;;
        --mysql-only)    RUN_ROODB=false ;;
        --skip-build)    SKIP_BUILD=true ;;
        --output=*)      OUTPUT_FILE="${arg#*=}" ;;
        --help|-h)
            sed -n '3,14p' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

ALL_WORKLOADS="oltp_point_select oltp_read_only oltp_read_write oltp_write_only"
if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS="$ALL_WORKLOADS"
else
    WORKLOADS="$WORKLOAD"
fi

# ─── Prereqs ──────────────────────────────────────────────────────────────────
check_prereq() {
    if ! command -v "$1" &>/dev/null; then
        echo "ERROR: $1 not found. Please install it."
        exit 1
    fi
}

check_prereq sysbench
check_prereq openssl
if [ "$RUN_ROODB" = true ]; then
    check_prereq cargo
fi
if [ "$RUN_MYSQL" = true ]; then
    check_prereq docker
fi

# ─── Temp dirs & cleanup ─────────────────────────────────────────────────────
TMPDIR_ROODB=""
ROODB_PID=""

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    # Stop RooDB
    if [ -n "$ROODB_PID" ] && kill -0 "$ROODB_PID" 2>/dev/null; then
        echo "Stopping RooDB (pid $ROODB_PID)"
        kill "$ROODB_PID" 2>/dev/null || true
        wait "$ROODB_PID" 2>/dev/null || true
    fi
    # Remove MySQL container
    if [ "$RUN_MYSQL" = true ]; then
        docker rm -f "$MYSQL_CONTAINER" &>/dev/null || true
    fi
    # Remove temp dirs
    if [ -n "$TMPDIR_ROODB" ] && [ -d "$TMPDIR_ROODB" ]; then
        rm -rf "$TMPDIR_ROODB"
    fi
}
trap cleanup EXIT

TMPDIR_ROODB="$(mktemp -d /tmp/roodb-bench-XXXX)"

# ─── Build RooDB ──────────────────────────────────────────────────────────────
if [ "$RUN_ROODB" = true ] && [ "$SKIP_BUILD" = false ]; then
    echo "=== Building RooDB (release) ==="
    (cd "$PROJECT_DIR" && cargo build --release)
fi

ROODB_BIN="$PROJECT_DIR/target/release/roodb"
ROODB_INIT_BIN="$PROJECT_DIR/target/release/roodb_init"

# ─── Generate TLS certs ──────────────────────────────────────────────────────
generate_certs() {
    local dir="$1"
    mkdir -p "$dir"
    # CA
    openssl req -x509 -newkey rsa:2048 -keyout "$dir/ca.key" -out "$dir/ca.crt" \
        -days 1 -nodes -subj "/CN=RooDB Bench CA" 2>/dev/null
    # Server cert
    openssl req -newkey rsa:2048 -keyout "$dir/server.key" -out "$dir/server.csr" \
        -nodes -subj "/CN=localhost" 2>/dev/null
    openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" \
        -CAcreateserial -out "$dir/server.crt" -days 1 \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null
    rm -f "$dir/server.csr" "$dir/ca.srl"
}

# ─── Wait for TCP port ───────────────────────────────────────────────────────
wait_for_port() {
    local host="$1" port="$2" label="$3" timeout="${4:-30}"
    local elapsed=0
    echo -n "Waiting for $label on port $port..."
    while ! (echo >/dev/tcp/"$host"/"$port") 2>/dev/null; do
        sleep 0.5
        elapsed=$((elapsed + 1))
        if [ "$elapsed" -ge "$((timeout * 2))" ]; then
            echo " TIMEOUT"
            echo "ERROR: $label did not become ready within ${timeout}s"
            return 1
        fi
    done
    echo " ready"
}

# ─── Wait for MySQL to accept queries ────────────────────────────────────────
wait_for_mysql() {
    local host="$1" port="$2" timeout="${3:-60}"
    echo -n "Waiting for MySQL to accept queries on port $port..."
    for i in $(seq 1 "$timeout"); do
        if mysql -h "$host" -P "$port" -u root -p"$BENCH_PASSWORD" -e "SELECT 1" &>/dev/null; then
            echo " ready (${i}s)"
            return 0
        fi
        sleep 1
    done
    echo " TIMEOUT"
    echo "ERROR: MySQL did not become ready within ${timeout}s"
    return 1
}

# ─── Start RooDB ──────────────────────────────────────────────────────────────
start_roodb() {
    local data_dir="$TMPDIR_ROODB/data"
    local cert_dir="$TMPDIR_ROODB/certs"

    generate_certs "$cert_dir"

    echo "=== Initializing RooDB ==="
    ROODB_ROOT_PASSWORD="$BENCH_PASSWORD" "$ROODB_INIT_BIN" --data-dir "$data_dir"

    echo "=== Starting RooDB on port $ROODB_PORT ==="
    "$ROODB_BIN" \
        --port "$ROODB_PORT" \
        --data-dir "$data_dir" \
        --cert-path "$cert_dir/server.crt" \
        --key-path "$cert_dir/server.key" \
        --raft-ca-cert-path "$cert_dir/ca.crt" &
    ROODB_PID=$!

    wait_for_port 127.0.0.1 "$ROODB_PORT" "RooDB"
}

# ─── Start MySQL ──────────────────────────────────────────────────────────────
start_mysql() {
    echo "=== Starting MySQL ($MYSQL_IMAGE) on port $MYSQL_PORT ==="
    docker rm -f "$MYSQL_CONTAINER" &>/dev/null || true
    docker run -d --name "$MYSQL_CONTAINER" \
        -e MYSQL_ROOT_PASSWORD="$BENCH_PASSWORD" \
        -e MYSQL_DATABASE="$BENCH_DB" \
        -p "$MYSQL_PORT:3306" \
        "$MYSQL_IMAGE" \
        --default-authentication-plugin=mysql_native_password >/dev/null

    wait_for_mysql 127.0.0.1 "$MYSQL_PORT" 60
}

# ─── Parse sysbench output ───────────────────────────────────────────────────
parse_sysbench() {
    local output="$1"
    local tps qps avg p95 p99 lat_min lat_max transactions errors

    # Use "per sec" to get the summary lines, not the "queries performed" subsection
    tps=$(echo "$output" | grep "transactions:.*per sec" | awk '{print $3}' | tr -d '(')
    qps=$(echo "$output" | grep "queries:.*per sec" | awk '{print $3}' | tr -d '(')
    avg=$(echo "$output" | grep "avg:" | awk '{print $2}')
    p95=$(echo "$output" | grep "95th percentile:" | awk '{print $3}')
    p99=$(echo "$output" | grep "99th percentile:" | awk '{print $3}')
    lat_min=$(echo "$output" | grep -E "^\s+min:" | awk '{print $2}')
    lat_max=$(echo "$output" | grep -E "^\s+max:" | awk '{print $2}')
    transactions=$(echo "$output" | grep "transactions:.*per sec" | awk '{print $2}')
    errors=$(echo "$output" | grep "ignored errors:" | awk '{print $3}')

    # Default to 0 if not found
    tps="${tps:-0}"
    qps="${qps:-0}"
    avg="${avg:-0}"
    p95="${p95:-0}"
    p99="${p99:-0}"
    lat_min="${lat_min:-0}"
    lat_max="${lat_max:-0}"
    transactions="${transactions:-0}"
    errors="${errors:-0}"

    cat <<EOF
{
  "tps": $tps,
  "qps": $qps,
  "latency_avg_ms": $avg,
  "latency_p95_ms": $p95,
  "latency_p99_ms": $p99,
  "latency_min_ms": $lat_min,
  "latency_max_ms": $lat_max,
  "transactions": $transactions,
  "errors": $errors
}
EOF
}

# ─── Run sysbench workload ───────────────────────────────────────────────────
run_sysbench() {
    local target="$1"    # mysql or roodb
    local workload="$2"
    local phase="$3"     # prepare, run, or cleanup
    local port host_args

    if [ "$target" = "mysql" ]; then
        port="$MYSQL_PORT"
        host_args="--mysql-host=127.0.0.1 --mysql-port=$port --mysql-user=root --mysql-password=$BENCH_PASSWORD --mysql-db=$BENCH_DB"
    else
        port="$ROODB_PORT"
        # Don't pass --mysql-ssl=on; RooDB advertises CLIENT_SSL and libmysqlclient
        # auto-upgrades. Explicit --mysql-ssl=on triggers SSL_CTX_set_default_verify_paths
        # which fails with self-signed certs in some sysbench builds.
        host_args="--mysql-host=127.0.0.1 --mysql-port=$port --mysql-user=root --mysql-password=$BENCH_PASSWORD --mysql-db=$BENCH_DB"
    fi

    timeout "$SYSBENCH_TIMEOUT" sysbench "$workload" \
        $host_args \
        --table-size="$TABLE_SIZE" \
        --tables="$TABLES" \
        --threads="$THREADS" \
        --time="$DURATION" \
        "$phase" 2>&1
}

# ─── Run one benchmark (returns JSON on stdout, progress on stderr) ──────────
run_benchmark() {
    local target="$1"
    local workload="$2"

    echo "--- $target: $workload prepare ---" >&2
    local prep_output
    if ! prep_output=$(run_sysbench "$target" "$workload" prepare); then
        echo "ERROR: $target $workload prepare failed" >&2
        echo "sysbench output: $prep_output" >&2
        echo '{"error": "prepare failed"}'
        return 0
    fi

    echo "--- $target: $workload run ---" >&2
    local output
    if ! output=$(run_sysbench "$target" "$workload" run); then
        echo "ERROR: $target $workload run failed" >&2
        echo "sysbench output: $output" >&2
        run_sysbench "$target" "$workload" cleanup >/dev/null 2>&1 || true
        echo '{"error": "run failed"}'
        return 0
    fi

    echo "--- $target: $workload cleanup ---" >&2
    run_sysbench "$target" "$workload" cleanup >/dev/null 2>&1 || true

    parse_sysbench "$output"
}

# ─── Print comparison table ──────────────────────────────────────────────────
print_comparison() {
    local workload="$1" mysql_json="$2" roodb_json="$3"

    echo ""
    echo "===== $workload ====="
    printf "%-20s %12s %12s\n" "" "MySQL" "RooDB"
    printf "%-20s %12s %12s\n" "---" "---" "---"

    if [ -n "$mysql_json" ] && [ -n "$roodb_json" ]; then
        local m_tps r_tps m_avg r_avg m_p95 r_p95 m_err r_err
        m_tps=$(echo "$mysql_json" | grep -o '"tps": [0-9.]*' | awk '{print $2}')
        r_tps=$(echo "$roodb_json" | grep -o '"tps": [0-9.]*' | awk '{print $2}')
        m_avg=$(echo "$mysql_json" | grep -o '"latency_avg_ms": [0-9.]*' | awk '{print $2}')
        r_avg=$(echo "$roodb_json" | grep -o '"latency_avg_ms": [0-9.]*' | awk '{print $2}')
        m_p95=$(echo "$mysql_json" | grep -o '"latency_p95_ms": [0-9.]*' | awk '{print $2}')
        r_p95=$(echo "$roodb_json" | grep -o '"latency_p95_ms": [0-9.]*' | awk '{print $2}')
        m_err=$(echo "$mysql_json" | grep -o '"errors": [0-9]*' | awk '{print $2}')
        r_err=$(echo "$roodb_json" | grep -o '"errors": [0-9]*' | awk '{print $2}')

        printf "%-20s %12s %12s\n" "TPS" "${m_tps:-n/a}" "${r_tps:-n/a}"
        printf "%-20s %12s %12s\n" "Avg Latency (ms)" "${m_avg:-n/a}" "${r_avg:-n/a}"
        printf "%-20s %12s %12s\n" "P95 Latency (ms)" "${m_p95:-n/a}" "${r_p95:-n/a}"
        printf "%-20s %12s %12s\n" "Errors" "${m_err:-n/a}" "${r_err:-n/a}"
    elif [ -n "$mysql_json" ]; then
        local m_tps m_avg m_p95 m_err
        m_tps=$(echo "$mysql_json" | grep -o '"tps": [0-9.]*' | awk '{print $2}')
        m_avg=$(echo "$mysql_json" | grep -o '"latency_avg_ms": [0-9.]*' | awk '{print $2}')
        m_p95=$(echo "$mysql_json" | grep -o '"latency_p95_ms": [0-9.]*' | awk '{print $2}')
        m_err=$(echo "$mysql_json" | grep -o '"errors": [0-9]*' | awk '{print $2}')
        printf "%-20s %12s %12s\n" "TPS" "${m_tps:-n/a}" "-"
        printf "%-20s %12s %12s\n" "Avg Latency (ms)" "${m_avg:-n/a}" "-"
        printf "%-20s %12s %12s\n" "P95 Latency (ms)" "${m_p95:-n/a}" "-"
        printf "%-20s %12s %12s\n" "Errors" "${m_err:-n/a}" "-"
    elif [ -n "$roodb_json" ]; then
        local r_tps r_avg r_p95 r_err
        r_tps=$(echo "$roodb_json" | grep -o '"tps": [0-9.]*' | awk '{print $2}')
        r_avg=$(echo "$roodb_json" | grep -o '"latency_avg_ms": [0-9.]*' | awk '{print $2}')
        r_p95=$(echo "$roodb_json" | grep -o '"latency_p95_ms": [0-9.]*' | awk '{print $2}')
        r_err=$(echo "$roodb_json" | grep -o '"errors": [0-9]*' | awk '{print $2}')
        printf "%-20s %12s %12s\n" "TPS" "-" "${r_tps:-n/a}"
        printf "%-20s %12s %12s\n" "Avg Latency (ms)" "-" "${r_avg:-n/a}"
        printf "%-20s %12s %12s\n" "P95 Latency (ms)" "-" "${r_p95:-n/a}"
        printf "%-20s %12s %12s\n" "Errors" "-" "${r_err:-n/a}"
    fi
}

# ─── Main ─────────────────────────────────────────────────────────────────────
echo "========================================"
echo "  RooDB Benchmark Suite"
echo "========================================"
echo "Workloads:  $WORKLOADS"
echo "Table size: $TABLE_SIZE"
echo "Tables:     $TABLES"
echo "Threads:    $THREADS"
echo "Duration:   ${DURATION}s"
echo "MySQL:      $RUN_MYSQL"
echo "RooDB:      $RUN_ROODB"
echo ""

# Start servers
if [ "$RUN_ROODB" = true ]; then
    start_roodb
fi
if [ "$RUN_MYSQL" = true ]; then
    start_mysql
fi

# Git info
GIT_COMMIT=$(cd "$PROJECT_DIR" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(cd "$PROJECT_DIR" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Initialize JSON
JSON_WORKLOADS=""

# Run benchmarks
for wl in $WORKLOADS; do
    echo ""
    echo "========================================"
    echo "  Workload: $wl"
    echo "========================================"

    mysql_result=""
    roodb_result=""

    if [ "$RUN_MYSQL" = true ]; then
        echo ""
        echo ">>> MySQL: $wl"
        mysql_result=$(run_benchmark mysql "$wl")
    fi

    if [ "$RUN_ROODB" = true ]; then
        # Check if RooDB server is still alive (may have crashed on previous workload)
        if [ -n "$ROODB_PID" ] && kill -0 "$ROODB_PID" 2>/dev/null; then
            echo ""
            echo ">>> RooDB: $wl"
            roodb_result=$(run_benchmark roodb "$wl")
        else
            echo ""
            echo ">>> RooDB: $wl (SKIPPED - server not running)"
            roodb_result='{"error": "server crashed"}'
        fi
    fi

    # Print comparison
    print_comparison "$wl" "$mysql_result" "$roodb_result"

    # Build JSON for this workload
    wl_json="\"$wl\": {"
    if [ -n "$mysql_result" ]; then
        wl_json+="\"mysql\": $mysql_result"
        if [ -n "$roodb_result" ]; then
            wl_json+=", "
        fi
    fi
    if [ -n "$roodb_result" ]; then
        wl_json+="\"roodb\": $roodb_result"
    fi
    wl_json+="}"

    if [ -n "$JSON_WORKLOADS" ]; then
        JSON_WORKLOADS+=", $wl_json"
    else
        JSON_WORKLOADS="$wl_json"
    fi
done

# ─── Write JSON output ───────────────────────────────────────────────────────
if [ -z "$OUTPUT_FILE" ]; then
    OUTPUT_FILE="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)_${GIT_COMMIT}.json"
fi

mkdir -p "$(dirname "$OUTPUT_FILE")"

cat > "$OUTPUT_FILE" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "git_commit": "$GIT_COMMIT",
  "git_branch": "$GIT_BRANCH",
  "config": {
    "table_size": $TABLE_SIZE,
    "tables": $TABLES,
    "threads": $THREADS,
    "duration": $DURATION
  },
  "workloads": {
    $JSON_WORKLOADS
  }
}
EOF

echo ""
echo "========================================"
echo "Results saved to: $OUTPUT_FILE"
echo "========================================"
