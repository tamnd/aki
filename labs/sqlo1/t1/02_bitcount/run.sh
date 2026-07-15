#!/usr/bin/env bash
# Sweep the popcount cache against a blob scan across bitmap sizes on
# the gate box, on both backend arms. The cold cache rows across sizes
# are the curve the verdict needs. Chunk column order is a SQLite-only
# question, so the pcfirst layout runs on store a alone and its delta
# against pclast decides whether the shipped schema moves pc ahead of
# the blob.
set -euo pipefail
cd "$(dirname "$0")"

out=bitcount.csv
echo "store,chunk_kib,layout,size_mb,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,file_mb,vmhwm_mb" >"$out"

for store in a b; do
	for size in 1 16 128 512; do
		echo "store=$store layout=pclast size=${size}MiB" >&2
		go run . -store "$store" -size "$size" >>"$out"
	done
done

for size in 1 16 128 512; do
	echo "store=a layout=pcfirst size=${size}MiB" >&2
	go run . -store a -layout pcfirst -size "$size" >>"$out"
done

echo "wrote $out" >&2
