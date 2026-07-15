#!/usr/bin/env bash
# Full chunkindex sweep. CPU and RAM only, no disk in the loop.
# The 1e9 counts arm needs ~32 MiB of counter state plus the measured
# directory itself (~380 MiB at 24M chunks), plan for ~1 GiB peak.
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-chunkindex-results.csv}"
bin=./chunkindex-lab
go build -o "$bin" .

"$bin" -header > "$out"

for n in 1000000 10000000 100000000 1000000000; do
  for policy in doc lf75 lf85; do
    "$bin" -arm occupancy -n "$n" -policy "$policy" -mode counts -seed 1 >> "$out"
  done
done

# Exact-mode cross-check at 1e7: same seed, real xxhash bit partition.
"$bin" -arm occupancy -n 10000000 -policy doc -mode exact -seed 1 >> "$out"

# Fingerprint false-hit rate against the exact table.
"$bin" -arm falsehit -n 10000000 -probes 2000000 -seed 1 >> "$out"

rm -f "$bin"
echo "wrote $out"
