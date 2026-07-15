#!/usr/bin/env bash
# Hot-tier-first vs page-cache-only at equal total budget. The work dir
# is kept across runs so the 2M-row preload is paid once per key shape.
# Usage: ./run.sh [outdir] (default: results in this directory)
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/sqlocache.csv"
work="${SQLOCACHE_DIR:-$(mktemp -d)}"

go build -o /tmp/sqlocache .

echo "budget_mib,arm,dist,keys,val,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,hit_pct,file_mb,vmhwm_mb" > "$out"

for dist in zipf uniform; do
  for budget in 64 256; do
    for arm in hot page; do
      echo "=== dist=$dist budget=${budget}MiB arm=$arm ===" >&2
      /tmp/sqlocache -dir "$work" -dist "$dist" -budget "$budget" -arm "$arm" >> "$out"
    done
  done
done

echo "wrote $out" >&2
