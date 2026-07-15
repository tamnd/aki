#!/bin/sh
# Full drivershoot sweep (spec 2064/sqlo1 doc 02 section 2): all three
# drivers across page size, value size, and key distribution. Writes one
# CSV; the verdict note in results/sqlo1/ reads from it. Run on the gate
# box for the numbers that count.
set -eu

out=${1:-/tmp/drivershoot.csv}
bindir=$(mktemp -d)
trap 'rm -rf "$bindir"' EXIT

echo "driver,workload,page,val,dist,ops,ns_per_op,ops_per_s" >"$out"
for tag in drv_modernc drv_zombiezen drv_ncruces; do
	go build -tags "$tag" -o "$bindir/shoot_$tag" .
done
for tag in drv_modernc drv_zombiezen drv_ncruces; do
	for page in 4096 8192 16384; do
		for val in 16 128 512; do
			for dist in uniform zipf; do
				echo "== $tag page=$page val=$val dist=$dist" >&2
				"$bindir/shoot_$tag" -page "$page" -val "$val" -dist "$dist" >>"$out"
			done
		done
	done
done
echo "wrote $out" >&2
