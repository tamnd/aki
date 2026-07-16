#!/usr/bin/env bash
# Sweep SPOP uniformity and the edit-vs-rebuild write cost across set
# sizes. Pass rule: the position allocator inside |z| < 3 with the even
# null far outside it, and the cost rows locate the pop fraction where
# rebuilding the remainder beats editing touched segments, the
# threshold slice 4 bakes.
set -euo pipefail
cd "$(dirname "$0")"

out=spop.csv
echo "kind,a,b,c,d,e,f,g,h,i,j,k" >"$out"

for members in 50000 200000 1000000; do
	echo "members=$members" >&2
	go run . -members "$members" >>"$out"
done

echo "wrote $out" >&2
echo "uniform rows: kind,arm,members,segs,trials,count,chi2_per_dof,zscore,ns_per_pop" >&2
echo "cost rows:    kind,count,pct,members,segs,touched,emptied,edit_frames,edit_bytes,rebuild_frames,rebuild_bytes,winner" >&2
