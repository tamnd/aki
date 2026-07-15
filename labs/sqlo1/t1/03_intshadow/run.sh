#!/usr/bin/env bash
# Sweep the INCR int64 shadow on and off, zipf and uniform, on the gate
# box, on both backend arms. The hot-incr delta between shadow arms is
# what the shadow buys; the cold-incr rows should show the store read
# drowning the parse either way.
set -euo pipefail
cd "$(dirname "$0")"

out=intshadow.csv
echo "store,arm,dist,keys,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,file_mb,vmhwm_mb" >"$out"

for store in a b; do
	for arm in shadow noshadow; do
		for dist in zipf uniform; do
			echo "store=$store arm=$arm dist=$dist" >&2
			go run . -store "$store" -arm "$arm" -dist "$dist" >>"$out"
		done
	done
done

echo "wrote $out" >&2
