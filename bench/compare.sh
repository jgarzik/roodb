#!/usr/bin/env bash
#
# Compare two RooDB benchmark result files
#
# Usage: bench/compare.sh results/run1.json results/run2.json

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <result1.json> <result2.json>"
    exit 1
fi

FILE1="$1"
FILE2="$2"

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq not found. Please install it."
    exit 1
fi

if [ ! -f "$FILE1" ]; then
    echo "ERROR: File not found: $FILE1"
    exit 1
fi
if [ ! -f "$FILE2" ]; then
    echo "ERROR: File not found: $FILE2"
    exit 1
fi

# Header
echo "========================================"
echo "  Benchmark Comparison"
echo "========================================"
echo ""
echo "Run 1: $FILE1"
echo "  Commit: $(jq -r '.git_commit' "$FILE1")  Branch: $(jq -r '.git_branch' "$FILE1")"
echo "  Time:   $(jq -r '.timestamp' "$FILE1")"
echo ""
echo "Run 2: $FILE2"
echo "  Commit: $(jq -r '.git_commit' "$FILE2")  Branch: $(jq -r '.git_branch' "$FILE2")"
echo "  Time:   $(jq -r '.timestamp' "$FILE2")"
echo ""

# Get workloads present in both files
WORKLOADS=$(jq -r '.workloads | keys[]' "$FILE1" | sort)

for wl in $WORKLOADS; do
    # Check if workload exists in both files
    if ! jq -e ".workloads[\"$wl\"]" "$FILE2" >/dev/null 2>&1; then
        continue
    fi

    # For each target (mysql, roodb)
    for target in mysql roodb; do
        if ! jq -e ".workloads[\"$wl\"][\"$target\"]" "$FILE1" >/dev/null 2>&1; then
            continue
        fi
        if ! jq -e ".workloads[\"$wl\"][\"$target\"]" "$FILE2" >/dev/null 2>&1; then
            continue
        fi

        # Check for errors
        err1=$(jq -r ".workloads[\"$wl\"][\"$target\"].error // empty" "$FILE1")
        err2=$(jq -r ".workloads[\"$wl\"][\"$target\"].error // empty" "$FILE2")
        if [ -n "$err1" ] || [ -n "$err2" ]; then
            echo "Workload: $wl ($target) - skipped (error in one or both runs)"
            continue
        fi

        echo "Workload: $wl ($target)"
        printf "%-20s %12s %12s %12s\n" "" "Run 1" "Run 2" "Delta"
        printf "%-20s %12s %12s %12s\n" "---" "---" "---" "---"

        for metric in tps latency_avg_ms latency_p95_ms latency_p99_ms errors; do
            v1=$(jq -r ".workloads[\"$wl\"][\"$target\"].$metric // 0" "$FILE1")
            v2=$(jq -r ".workloads[\"$wl\"][\"$target\"].$metric // 0" "$FILE2")

            # Compute delta
            if [ "$metric" = "errors" ]; then
                v1_int=${v1%.*}
                v2_int=${v2%.*}
                if [ "$v1_int" -eq 0 ] && [ "$v2_int" -eq 0 ]; then
                    delta="-"
                else
                    delta="$((v2_int - v1_int))"
                fi
            else
                if command -v bc &>/dev/null && [ "$(echo "$v1 > 0" | bc -l 2>/dev/null)" = "1" ]; then
                    pct=$(echo "scale=1; ($v2 - $v1) * 100 / $v1" | bc -l 2>/dev/null || echo "n/a")
                    if [ "$pct" != "n/a" ]; then
                        # Add + prefix for positive
                        if echo "$pct" | grep -qv "^-"; then
                            pct="+${pct}"
                        fi
                        delta="${pct}%"
                    else
                        delta="n/a"
                    fi
                else
                    delta="n/a"
                fi
            fi

            # Label
            case "$metric" in
                tps)              label="TPS" ;;
                latency_avg_ms)   label="Avg Latency (ms)" ;;
                latency_p95_ms)   label="P95 Latency (ms)" ;;
                latency_p99_ms)   label="P99 Latency (ms)" ;;
                errors)           label="Errors" ;;
            esac

            printf "%-20s %12s %12s %12s\n" "$label" "$v1" "$v2" "$delta"
        done
        echo ""
    done
done
