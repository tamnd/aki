#!/usr/bin/env bash
# Sweep the segment split threshold across field-size distributions and
# HSET:HGET ratios on the gate box, on both backend arms. The hset
# row's wal_b_per_op column is the W4 bandwidth bill each threshold
# signs; the flush row carries the drain IO.
set -euo pipefail
cd "$(dirname "$0")"

out=hseg.csv
echo "store,seg_b,fdist,setpct,dist,keys,fields,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,wa,wal_b_per_op,file_mb,wal_mb,vmhwm_mb" >"$out"

for store in a b; do
	for seg in 2016 4032 8064; do
		for fdist in small med large; do
			for setpct in 10 50 90; do
				echo "store=$store seg=$seg fdist=$fdist setpct=$setpct" >&2
				go run . -store "$store" -seg "$seg" -fdist "$fdist" -setpct "$setpct" >>"$out"
			done
		done
	done
done

echo "wrote $out" >&2
