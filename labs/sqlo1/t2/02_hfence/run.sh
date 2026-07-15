#!/usr/bin/env bash
# Draw the point-op flatness curve across the ladder on the gate box,
# on both backend arms. The verdict wants ns_per_op and rec_reads flat
# from 10^2 to 10^8 at each tier's ceiling; the 10^9 point (~56 GB on
# disk) is a separate long run per arm:
# go run . -store b -fields 1000000000 -dir /path/with/space
set -euo pipefail
cd "$(dirname "$0")"

out=hfence.csv
echo "store,fields,mode,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,rec_reads,root_b,fence_mb,file_mb,vmhwm_mb" >"$out"

for store in a b; do
	for fields in 30 100 10000 1000000 100000000; do
		echo "store=$store fields=$fields" >&2
		go run . -store "$store" -fields "$fields" >>"$out"
	done
done

echo "wrote $out" >&2
