#!/usr/bin/env bash
# ApplyBatch sizing sweep and the A2 stack-tax prediction arms, on the
# real sqlo1a stack. One invocation carries everything; the batch-size
# sweep is internal.
# Usage: ./run.sh [outdir] (default: results in this directory)
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/abatch.csv"
work="${ABATCH_DIR:-$(mktemp -d)}"

go build -o /tmp/abatch .

echo "keys,val,workload,batch,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,vmhwm_mb" > "$out"
/tmp/abatch -dir "$work" >> "$out"
rm -f "$work"/abatch.db "$work"/abatch.db-wal "$work"/abatch.db-shm

echo "wrote $out" >&2
