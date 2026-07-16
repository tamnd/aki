#!/usr/bin/env bash
# Sweep the two zset split thresholds across the three doc 09 mixes.
# Pass rule: a defensible (mem_max, run_max) pair where the zadd row's
# WAL bill and the zrange row's runs-per-window cross, and a measured
# frames-per-move and bytes-per-move input for PRED-SQLO1-T4-WALZ.
set -euo pipefail
cd "$(dirname "$0")"

out=hsegz.csv
echo "mix,memmax,runmax,workload,ops,ns_op,p50_ns,p99_ns,frames_op,wal_b_op,runs_op,read_b_op,x1,x2,x3" >"$out"

for mix in zaddheavy zrangeheavy board; do
	for mm in 2016 4032 8064; do
		for rm in 2016 4032 8064; do
			echo "mix=$mix memmax=$mm runmax=$rm" >&2
			go run . -mix "$mix" -memmax "$mm" -runmax "$rm" >>"$out"
		done
	done
done

echo "wrote $out" >&2
echo "zadd row:   frames_op and wal_b_op average over score-moving ZADDs; x1 = same-run move share, x2 = splits per 1000 moves, x3 = structural (fence-shape) bills per 1000 moves" >&2
echo "zrange row: runs_op = runs touched per 100-element window, read_b_op = encoded bytes a cold read of them pulls" >&2
echo "shape row:  ops = cardinality, runs_op = entries per run, read_b_op = members per segment, x1 = segments, x2 = runs, x3 = fence bytes" >&2
echo "drain row:  ops = drains, frames_op = rows per drain, wal_b_op = write amplification (drained over logical bytes)" >&2
