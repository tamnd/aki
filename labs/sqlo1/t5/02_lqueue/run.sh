#!/usr/bin/env bash
# The queue marquee: sustained LPUSH+RPOP throughput and p99 vs depth.
# Default is the aki arm served in process over the real sqlo1b file
# store; point ADDR and LABEL at a running redis-server or
# valkey-server for a rival arm (the gate box does, per rival, after
# starting the server with appendonly yes to keep the durability
# story comparable).
#
# DEPTHS stops at 1000 until the fence paging slice lands: a 200 B
# queue caps at ~3000 elements on the flat fence and the harness
# fails loudly past it. The full 10 to 10^7 sweep is the gate-box run
# queued behind that slice.
set -euo pipefail
cd "$(dirname "$0")"

out=lqueue.csv
DEPTHS=${DEPTHS:-"10 100 1000"}
ELEM=${ELEM:-200}
CONNS=${CONNS:-8}
WARM=${WARM:-5s}
DUR=${DUR:-20s}

if [ ! -f "$out" ]; then
	echo "server,depth,elem,conns,secs,ops,ops_s,push_p50_us,push_p99_us,pop_p50_us,pop_p99_us,misses" >"$out"
fi

for d in $DEPTHS; do
	if [ -n "${ADDR:-}" ]; then
		echo "server=${LABEL:-rival} depth=$d" >&2
		go run . -mode dial -addr "$ADDR" -server "${LABEL:-rival}" \
			-depth "$d" -elem "$ELEM" -conns "$CONNS" -warm "$WARM" -dur "$DUR" >>"$out"
	else
		echo "server=aki depth=$d" >&2
		go run . -mode serve -store file -server aki \
			-depth "$d" -elem "$ELEM" -conns "$CONNS" -warm "$WARM" -dur "$DUR" >>"$out"
	fi
done
