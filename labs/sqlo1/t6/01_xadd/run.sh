#!/usr/bin/env bash
# Sweep the stream run cut thresholds across the doc 10 mixes.
# Pass rule: a defensible (run_max, ecap) pair where the append row's
# WAL bill and the feed row's amortized drop bill cross, a measured
# bytes-per-XADD input for PRED-SQLO1-T6-XADD, and the encode row
# proving the name table plus varint deltas earn their complexity
# against the naive encoding.
set -euo pipefail
cd "$(dirname "$0")"

out=xadd.csv
echo "mix,runmax,ecap,elen,nstreams,workload,ops,ns_op,frames_op,wal_b_op,x1,x2,x3,x4" >"$out"

for mix in append feed encode; do
	for rm in 2016 4032 8064; do
		for ec in 64 128 256; do
			for el in 100 1000 4096; do
				echo "mix=$mix runmax=$rm ecap=$ec elen=$el" >&2
				go run . -mix "$mix" -runmax "$rm" -ecap "$ec" -elen "$el" >>"$out"
			done
		done
	done
done

for ns in 10 100 1000; do
	for rm in 2016 4032 8064; do
		echo "mix=fanout nstreams=$ns runmax=$rm" >&2
		go run . -mix fanout -nstreams "$ns" -runmax "$rm" >>"$out"
	done
done

echo "wrote $out" >&2
echo "append row: frames_op and wal_b_op average over pure auto-ID XADD; x1 = run cuts per 1000 ops, x2 = structural (fence-shape) bills per 1000 ops" >&2
echo "feed row:   same columns for XADD MAXLEN ~ pairs; x1 = whole-run drops per 1000 ops, x2 = structural bills per 1000 ops, x3 = steady length" >&2
echo "fanout row: same columns round-robining capped streams; the drain row underneath is the point" >&2
echo "encode row: ns_op = encode ns per entry over sealed runs; x1 = encoded bytes per entry, x2 = naive bytes per entry, x3 = ratio, x4 = runs" >&2
echo "shape row:  ops = length, x1 = entries per run, x2 = bytes per run, x3 = runs, x4 = paged" >&2
echo "drain row:  ops = drains, frames_op = total frames, wal_b_op = write amplification (drained over logical bytes), x1 = residual dirty bytes" >&2
