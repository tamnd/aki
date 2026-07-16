#!/usr/bin/env bash
# Sweep the SINTER probe-vs-merge cost across driver/target ratios and
# cache temperatures, the gather window, and the SUNION dedupe digest
# inputs. Pass rule: probe wins clearly while driver count sits under
# the target's fence length, merge wins clearly past it, and the
# 128-bit digest stays negligible at 10^9 uniques.
set -euo pipefail
cd "$(dirname "$0")"

out=salgebra.csv
echo "kind,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o" >"$out"

for tmembers in 200000 1000000; do
	echo "tmembers=$tmembers" >&2
	go run . -tmembers "$tmembers" >>"$out"
done

echo "wrote $out" >&2
echo "ratio rows:   kind,dmembers,dsegs,tmembers,tsegs,d_over_tsegs,hot,touched,probe_reads,probe_rounds,probe_bytes,merge_reads,merge_rounds,merge_bytes,probes_per_round,winner" >&2
echo "window rows:  kind,dmembers,tmembers,window_segs,touched,reads,rounds" >&2
echo "collide rows: kind,uniques,digest_bits,p_any_collision" >&2
echo "dedupe rows:  kind,uniques,width_bytes,bytes_per_unique" >&2
