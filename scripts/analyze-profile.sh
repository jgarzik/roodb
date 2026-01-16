#!/bin/bash
#
# analyze-profile.sh - Extract hot spots from samply profile
#
# Usage: ./scripts/analyze-profile.sh [profile.json.gz]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find profile
if [[ $# -ge 1 ]]; then
    PROFILE="$1"
else
    PROFILE=$(ls -t "$REPO_ROOT"/profiles/roodb_profile_*.json.gz 2>/dev/null | head -1)
fi

if [[ -z "$PROFILE" || ! -f "$PROFILE" ]]; then
    echo "Error: No profile found. Run perf-profile.sh first." >&2
    exit 1
fi

# Find test binary
TEST_BIN=$(find "$REPO_ROOT/target/release/deps" -name 'suite-*' -executable -type f ! -name '*.d' 2>/dev/null | head -1)

# Decompress
TEMP_JSON=$(mktemp)
trap "rm -f $TEMP_JSON" EXIT
gunzip -c "$PROFILE" > "$TEMP_JSON"

python3 - "$TEST_BIN" "$TEMP_JSON" << 'PYEOF'
import json
import subprocess
import sys
from collections import defaultdict

TEST_BIN = sys.argv[1] if len(sys.argv) > 1 else None
JSON_FILE = sys.argv[2] if len(sys.argv) > 2 else '/tmp/profile.json'

print(f"Profile:     {JSON_FILE}")
print(f"Test binary: {TEST_BIN}")

with open(JSON_FILE, 'r') as f:
    data = json.load(f)

libs = data.get('libs', [])

# Find suite binary index
suite_idx = None
for i, lib in enumerate(libs):
    if 'suite' in lib.get('name', ''):
        suite_idx = i
        break

# Collect samples
addr_samples = defaultdict(int)
lib_samples = defaultdict(int)
total = 0
suite_total = 0

for thread in data.get('threads', []):
    resource_table = thread.get('resourceTable', {})
    func_table = thread.get('funcTable', {})
    frame_table = thread.get('frameTable', {})
    stack_table = thread.get('stackTable', {})
    samples = thread.get('samples', {})

    resource_libs = resource_table.get('lib', [])
    func_resources = func_table.get('resource', [])
    frame_funcs = frame_table.get('func', [])
    frame_addrs = frame_table.get('address', [])
    stack_frames = stack_table.get('frame', [])
    sample_stacks = samples.get('stack', [])

    for stack_idx in sample_stacks:
        if stack_idx is None:
            continue
        total += 1

        if stack_idx < len(stack_frames):
            frame_idx = stack_frames[stack_idx]
            if frame_idx < len(frame_funcs) and frame_idx < len(frame_addrs):
                func_idx = frame_funcs[frame_idx]
                addr = frame_addrs[frame_idx]
                if func_idx < len(func_resources):
                    res_idx = func_resources[func_idx]
                    if res_idx >= 0 and res_idx < len(resource_libs):
                        lib_idx = resource_libs[res_idx]
                        lib_name = libs[lib_idx].get('name', 'unknown') if lib_idx < len(libs) else 'unknown'
                        lib_samples[lib_name] += 1
                        if lib_idx == suite_idx and addr > 0:
                            addr_samples[addr] += 1
                            suite_total += 1

print(f"\nTotal samples: {total}")
print()

# Library breakdown
print("=" * 80)
print("SAMPLES BY LIBRARY")
print("=" * 80)
for name, count in sorted(lib_samples.items(), key=lambda x: -x[1])[:10]:
    pct = 100.0 * count / total if total > 0 else 0
    print(f"{count:>8} {pct:>5.1f}%  {name}")

# Top functions from suite binary
if TEST_BIN and addr_samples:
    top_addrs = sorted(addr_samples.items(), key=lambda x: -x[1])[:40]
    addr_list = [f"0x{addr:x}" for addr, _ in top_addrs]

    try:
        result = subprocess.run(
            ['addr2line', '-f', '-C', '-e', TEST_BIN] + addr_list,
            capture_output=True, text=True, timeout=60
        )
        lines = result.stdout.strip().split('\n')
        funcs = [lines[i] for i in range(0, len(lines), 2)]
    except Exception as e:
        print(f"\naddr2line error: {e}")
        funcs = addr_list

    print()
    print("=" * 80)
    print("TOP HOT FUNCTIONS (self time)")
    print("=" * 80)
    print(f"{'Samples':>8} {'%':>6}  Function")
    print("-" * 80)

    for i, (addr, count) in enumerate(top_addrs):
        pct = 100.0 * count / suite_total if suite_total > 0 else 0
        func = funcs[i] if i < len(funcs) else f"0x{addr:x}"
        print(f"{count:>8} {pct:>5.1f}%  {func[:70]}")
PYEOF
