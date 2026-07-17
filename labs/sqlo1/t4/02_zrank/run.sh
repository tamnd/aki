#!/usr/bin/env bash
# Sweep fence shapes across the cardinality decades. Pass rule: a
# shape whose rank walk stays flat to 10^9 with a root the per-command
# bill can afford, plus the strict and drain-coalesced move bills that
# feed PRED-SQLO1-T4-RANK and the paged half of PRED-SQLO1-T4-WALZ.
set -euo pipefail
cd "$(dirname "$0")"

out=zrank.csv
echo "shape,n,runs,levels,root_ents,root_kb,rank_p50_ns,rank_p99_ns,cold_recs,cold_kb,mv_kb_strict,mv_kb_defer,pattern,window,sink" >"$out"

# The flat fence is the slice 3 status quo; past 10^6 its walk is
# minutes per sweep cell and the point is already made.
for n in 1000 10000 100000 1000000; do
	go run . -shape flat -n "$n" >>"$out"
done

for sh in p250 p1000 p250x250 p128x128 p512x512; do
	for n in 1000 100000 10000000 100000000 1000000000; do
		for pat in uniform board; do
			echo "shape=$sh n=$n pattern=$pat" >&2
			go run . -shape "$sh" -n "$n" -pattern "$pat" >>"$out"
		done
	done
done

echo "wrote $out" >&2
