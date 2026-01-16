#!/bin/bash
#
# perf-profile.sh - Headless CPU profiling for RooDB using samply
#
# Profiles the RooDB server by running integration tests under samply.
# Outputs a Firefox Profiler compatible profile for later analysis.
#
# Prerequisites:
#   - samply installed (cargo install samply)
#   - Linux with perf_event access (may need: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
OUTPUT_DIR="$REPO_ROOT/profiles"
TEST_FILTER="roodb_suite"
TEST_THREADS=1
SAMPLE_RATE=1000

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Profile RooDB server performance using samply (headless).

Options:
    -o, --output DIR      Output directory for profiles (default: ./profiles)
    -f, --filter FILTER   Test filter pattern (default: roodb_suite)
    -t, --threads N       Test threads (default: 1, serial for cleaner profile)
    -r, --rate HZ         Sampling rate in Hz (default: 1000)
    -h, --help            Show this help

Examples:
    $(basename "$0")                          # Profile all integration tests
    $(basename "$0") -f roodb_suite::dml      # Profile only DML tests
    $(basename "$0") -t 4                     # Run tests in parallel
    $(basename "$0") -o /tmp/profiles         # Custom output directory

Output:
    Profiles saved as: profiles/roodb_profile_YYYYMMDD_HHMMSS.json.gz
    View with: samply load <profile.json.gz>
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -f|--filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        -t|--threads)
            TEST_THREADS="$2"
            shift 2
            ;;
        -r|--rate)
            SAMPLE_RATE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

# Check prerequisites
if ! command -v samply &> /dev/null; then
    echo "Error: samply not found. Install with: cargo install samply" >&2
    exit 1
fi

# Check perf_event_paranoid
PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "0")
if [[ "$PARANOID" -gt 1 ]]; then
    echo "Warning: perf_event_paranoid=$PARANOID (need <=1 for non-root profiling)" >&2
    echo "Fix with: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid" >&2
    echo ""
fi

# Setup output
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE="$OUTPUT_DIR/roodb_profile_$TIMESTAMP.json.gz"

cd "$REPO_ROOT"

echo "=== RooDB Performance Profiling ==="
echo "Output:      $PROFILE"
echo "Test filter: $TEST_FILTER"
echo "Threads:     $TEST_THREADS"
echo "Sample rate: ${SAMPLE_RATE}Hz"
echo ""

# Build with debug symbols FIRST (not under samply)
echo "Building tests with debug symbols..."
export CARGO_PROFILE_RELEASE_DEBUG=2
cargo test --release --no-run 2>&1 | tail -5

# Find the test binary
TEST_BIN=$(find target/release/deps -name 'suite-*' -executable -type f ! -name '*.d' 2>/dev/null | head -1)
if [[ -z "$TEST_BIN" ]]; then
    echo "Error: Could not find test binary in target/release/deps/" >&2
    exit 1
fi
echo "Test binary: $TEST_BIN"

echo ""
echo "Profiling tests (this may take a while)..."
echo ""

# Profile ONLY the test binary (not cargo)
samply record \
    --save-only \
    --no-open \
    --rate "$SAMPLE_RATE" \
    --output "$PROFILE" \
    -- \
    "$TEST_BIN" "$TEST_FILTER" --test-threads="$TEST_THREADS"

echo ""
echo "=== Profiling Complete ==="
echo "Profile: $PROFILE"
echo "Size:    $(du -h "$PROFILE" | cut -f1)"
echo ""
echo "To analyze: samply load $PROFILE"
echo "Or run:     $SCRIPT_DIR/analyze-profile.sh $PROFILE"
