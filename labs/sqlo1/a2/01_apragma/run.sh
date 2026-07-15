#!/usr/bin/env bash
# Sweep page size, cache budget, and checkpoint cadence for the frozen
# ncruces driver on a dataset that beats the page cache.
# Usage: ./run.sh [outdir] (default: results in this directory)
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/apragma.csv"
work="${APRAGMA_DIR:-$(mktemp -d)}"

go build -o /tmp/apragma .

echo "page,cache_kib,ckpt_every,keys,val,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,file_mb,wal_mb,vmhwm_mb" > "$out"

for page in 4096 8192 16384; do
  for cache in 8192 32768 131072; do
    for ckpt in 1 8 64; do
      echo "=== page=$page cache=${cache}KiB ckpt=$ckpt ===" >&2
      /tmp/apragma -dir "$work" -page "$page" -cache "$cache" -ckpt "$ckpt" >> "$out"
      rm -f "$work"/apragma-*.db "$work"/apragma-*.db-wal "$work"/apragma-*.db-shm
    done
  done
done

echo "wrote $out" >&2
