#!/usr/bin/env bash
# Sweep the HRANDFIELD weighting arms across field counts. Pass rule:
# exact and fill15 inside |z| < 3, unweighted far outside it; quant4's
# z at the big counts decides whether 4 bits of fill class could ever
# be enough, with 15 the shipped width.
set -euo pipefail
cd "$(dirname "$0")"

out=hrand.csv
echo "arm,fields,segs,samples,chi2_per_dof,zscore,max_seg_dev_pct,ns_per_draw" >"$out"

for fields in 50000 200000 1000000; do
	echo "fields=$fields" >&2
	go run . -fields "$fields" >>"$out"
done

echo "wrote $out" >&2
