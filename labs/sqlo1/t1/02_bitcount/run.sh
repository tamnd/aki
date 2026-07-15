#!/usr/bin/env bash
# Sweep the popcount cache against a blob scan across bitmap sizes and
# both chunk column orders on the gate box. The cold cache rows across
# sizes are the curve the verdict needs, and the pclast/pcfirst delta
# decides whether the shipped schema moves pc ahead of the blob.
set -euo pipefail
cd "$(dirname "$0")"

out=bitcount.csv
echo "chunk_kib,layout,size_mb,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,file_mb,vmhwm_mb" >"$out"

for layout in pclast pcfirst; do
	for size in 1 16 128 512; do
		echo "layout=$layout size=${size}MiB" >&2
		go run . -layout "$layout" -size "$size" >>"$out"
	done
done

echo "wrote $out" >&2
