#!/usr/bin/env bash
# Sweep the list node split thresholds across the four doc 07 mixes.
# Pass rule: a defensible (node_max, ecap) pair where the queue row's
# WAL bill and the range row's nodes-per-window cross, a measured
# bytes-per-op input for PRED-SQLO1-T5-QUEUE, and the feed row's
# amortized bill for PRED-SQLO1-T5-FEED.
set -euo pipefail
cd "$(dirname "$0")"

out=lnode.csv
echo "mix,nodemax,ecap,workload,ops,ns_op,p50_ns,p99_ns,frames_op,wal_b_op,nodes_op,read_b_op,x1,x2,x3" >"$out"

for mix in deque dequeid feed page; do
	for nm in 2016 4032 8064; do
		for ec in 64 128 256; do
			echo "mix=$mix nodemax=$nm ecap=$ec" >&2
			go run . -mix "$mix" -nodemax "$nm" -ecap "$ec" >>"$out"
		done
	done
done

echo "wrote $out" >&2
echo "queue row: frames_op and wal_b_op average over the 50/50 push-pop stream; x1 = node cuts per 1000 ops, x2 = node drops per 1000 ops, x3 = structural (fence-shape) bills per 1000 ops" >&2
echo "feed row:  same columns for LPUSH-plus-LTRIM pairs; x1 = whole-node drops per 1000 ops, x2 = edge rewrites per 1000 ops, x3 = structural bills per 1000 ops" >&2
echo "range row: nodes_op = nodes touched per 100-element window, read_b_op = encoded bytes a cold read of them pulls; index row = LINDEX seek latency" >&2
echo "shape row: ops = length, nodes_op = elems per node, read_b_op = bytes per node, x1 = nodes, x2 = fence bytes, x3 = paged" >&2
echo "drain row: ops = drains, frames_op = total frames, wal_b_op = write amplification (drained over logical bytes)" >&2
