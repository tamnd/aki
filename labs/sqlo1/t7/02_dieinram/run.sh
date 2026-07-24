#!/bin/sh
# Die-in-RAM sweep: TTL against the drain interval, plus a uniform-TTL
# mix and a half-volatile arm. At 8 MB/s simulated influx the 8 MiB
# dirty threshold makes the drain interval 1000 ms by construction.
set -eu
cd "$(dirname "$0")"

bin=/tmp/lab-dieinram
go build -o "$bin" .
work=$(mktemp -d /tmp/dieinram.XXXXXX)
trap 'rm -rf "$work"' EXIT

out=dieinram.csv
echo "arm,ttl_ms,vol_pct,interval_ms,written,drained_alive,drained_dead,died_in_ram,pending,slack_le_1,slack_le_2,lag_p50_ms,pct_drained_dead,pct_died_ram,pct_reapcancel,pct_reorder_1iv,pct_reorder_2iv,wall_s" > "$out"

for ttl in 250 500 1000 2000 4000 8000; do
    rm -f "$work"/dieinram.aki*
    "$bin" -dir "$work" -arm "ratio" -ttl "$ttl" >> "$out"
    echo "ratio ttl=${ttl}ms done" >&2
done

rm -f "$work"/dieinram.aki*
"$bin" -dir "$work" -arm "uniform" -ttl 2000 -uniform >> "$out"
echo "uniform done" >&2

rm -f "$work"/dieinram.aki*
"$bin" -dir "$work" -arm "vol50" -ttl 1000 -vol 50 >> "$out"
echo "vol50 done" >&2

column -s, -t "$out" >&2
