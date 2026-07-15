#!/usr/bin/env bash
# Sweep rope chunk size across the four doc 05 operator mixes on the
# gate box, on both backend arms. Uniform offsets are the coalescing
# worst case and run everywhere; the SETBIT mix adds a zipf arm because
# presence bitmaps and rollups concentrate on hot chunks and that is
# where a bigger chunk earns its write amplification back.
set -euo pipefail
cd "$(dirname "$0")"

out=ropechunk.csv
echo "store,chunk_kib,mix,dist,keys,val_mb,workload,ops,ns_per_op,ops_per_s,p50_ns,p99_ns,max_ns,wa,file_mb,wal_mb,vmhwm_mb" >"$out"

for store in a b; do
	for chunk in 8 16 32 64; do
		for mix in setrange append setbit getrange; do
			echo "store=$store chunk=${chunk}KiB mix=$mix dist=uniform" >&2
			go run . -store "$store" -chunk "$chunk" -mix "$mix" -dist uniform >>"$out"
		done
		echo "store=$store chunk=${chunk}KiB mix=setbit dist=zipf" >&2
		go run . -store "$store" -chunk "$chunk" -mix setbit -dist zipf >>"$out"
	done
done

echo "wrote $out" >&2
