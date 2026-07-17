#!/usr/bin/env bash
# Sweep the cover arms across radii and latitude bands. Pass rule:
# the arm slice 11 bakes must hold the over-read ratio and the runs
# read per search jointly best at the common radii, and the parity
# phase must report zero mismatches against a live Redis 8.8.0.
set -euo pipefail
cd "$(dirname "$0")"

out=geo.csv
echo "dataset,n,lat,radius_m,arm,step,cells,cands,results,overread,runs,us_p50,us_p99" >"$out"

for ds in uniform cluster; do
	for lat in 0 45 70; do
		for r in 100 500 1000 5000 10000 50000 100000 500000; do
			for arm in coarse redis fine; do
				echo "ds=$ds lat=$lat r=$r arm=$arm" >&2
				go run . -dataset "$ds" -lat "$lat" -radius "$r" -arm "$arm" >>"$out"
			done
		done
	done
done

# Scale check: does the trade move at 10^7 points?
for r in 100 1000 10000 100000 500000; do
	echo "scale n=1e7 r=$r" >&2
	go run . -dataset uniform -n 10000000 -lat 45 -radius "$r" -arm redis >>"$out"
	go run . -dataset uniform -n 10000000 -lat 45 -radius "$r" -arm fine >>"$out"
done

port="${REDIS_PORT:-7799}"
if redis-cli -p "$port" ping >/dev/null 2>&1; then
	go run . -parity -port "$port" >&2
else
	echo "parity SKIPPED: no redis on port $port" >&2
fi

echo "wrote $out" >&2
