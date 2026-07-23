#!/usr/bin/env bash
# Sweep the sampling reaper's chunk budget on warm and cold indexes
# plus the tombstone batch size, over the real sqlo1b store.
# Usage: ./run.sh [outdir] (default: results in this directory)
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/reaper.csv"
work="${REAPER_DIR:-$(mktemp -d)}"

go build -o /tmp/reaper-lab .

echo "arm,param,keys,near_pct,mid_pct,far_pct,expired_pct,passes,p50_us,p99_us,max_us,entries_per_pass,probes,expired_found,expired_true,total_ms" > "$out"

for keys in 200000 1000000; do
  echo "=== keys=$keys ===" >&2
  /tmp/reaper-lab -dir "$work" -keys "$keys" >> "$out"
  rm -f "$work"/reaper.aki "$work"/reaper.aki.aki-wal
done

# The non-volatile control: all class none, where the skip must make
# laps nearly free of probes.
echo "=== control: no TTLs ===" >&2
/tmp/reaper-lab -dir "$work" -keys 200000 -near 0 -mid 0 -far 0 >> "$out"
rm -f "$work"/reaper.aki "$work"/reaper.aki.aki-wal

echo "wrote $out" >&2
