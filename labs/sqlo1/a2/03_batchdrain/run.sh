#!/usr/bin/env bash
# Sweep the drain window (dirty-bytes threshold) and per-cycle op cap
# over the real sqlo1a store, for both write distributions.
# Usage: ./run.sh [outdir] (default: results in this directory)
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/batchdrain.csv"
work="${BATCHDRAIN_DIR:-$(mktemp -d)}"

go build -o /tmp/batchdrain .

echo "threshold_mib,maxops,dist,keys,val,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,mb_a,mb_b,vmhwm_mb" > "$out"

for dist in uniform zipf; do
  for threshold in 1 4 8 16 32; do
    for maxops in 256 1024 4096; do
      echo "=== dist=$dist threshold=${threshold}MiB maxops=$maxops ===" >&2
      /tmp/batchdrain -dir "$work" -dist "$dist" -threshold "$threshold" -maxops "$maxops" >> "$out"
      rm -f "$work"/batchdrain.db "$work"/batchdrain.db-wal "$work"/batchdrain.db-shm
    done
  done
done

echo "wrote $out" >&2
