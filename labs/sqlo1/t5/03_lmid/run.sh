#!/usr/bin/env bash
# Sweep the lazy merge threshold under the middle-insert storm and the
# steady-length churn. Pass rule: the churn's node count must stay
# bounded (nodes_op near 1.0) at some merge_max with an acceptable WAL
# surcharge over the no-merge arm, or the doc 14 kill-table row for
# the list type triggers.
set -euo pipefail
cd "$(dirname "$0")"

out=lmid.csv
echo "mix,nodemax,mergemax,workload,ops,ns_op,frames_op,wal_b_op,nodes_op,x1,x2,x3" >"$out"

for mix in storm churn; do
	for mm in 0 1008 2016 3024; do
		echo "mix=$mix mergemax=$mm" >&2
		go run . -mix "$mix" -mergemax "$mm" >>"$out"
	done
done

# The decimation adversary runs at a shorter length so 200k ops cover
# ~10 erosion rounds.
for mm in 0 1008 2016 3024; do
	echo "mix=decimate mergemax=$mm" >&2
	go run . -mix decimate -length 20000 -mergemax "$mm" >>"$out"
done

echo "wrote $out" >&2
echo "storm row: fixed-position inserts on a growing list; nodes_op = nodes at end, x1 = splits per 1000 ops, x2 = merges per 1000 ops, x3 = end occupancy" >&2
echo "churn row: insert-remove pairs at steady length; frames_op and wal_b_op are per single op, nodes_op = end nodes over start nodes, x1 = splits per 1000 pairs, x2 = merges per 1000 pairs, x3 = end occupancy" >&2
echo "decimate row: halve-then-refill-at-a-point rounds; same columns as churn" >&2
echo "shape rows: ops = length, nodes_op = nodes, x1 = occupancy, x2 = fence bytes, x3 = paged" >&2
