#!/bin/bash
# Lab 27: the per-hop-node reply-buffer headroom as a write-heavy memory lever.
#
# A hop node starts its reply buffer at repCap = batchDataCap + 64*batchCap
# (tuning.go): the 64*batchCap term is reply headroom so the steady path never
# grows the buffer. But the buffer grows on demand, so a write-heavy load never
# needs it: SET replies are +OK, five bytes, and a GET of a 64B value fits the
# node's data cap alone. The headroom is pure slack on the point surface, and at
# c512 fan-out every pooled node on every connection carries it. This lab sweeps
# the new -rep-cap override at the gate config to measure the write-heavy VmHWM
# it recovers and confirm it costs no throughput.
#
# It is rep-cap-internal (one knob, one harness, one box session), so no rival
# ratios are computed here; the slice's results/ run pairs the winning value
# against the CF16-frozen rivals. Run it on the GamingPC gate box under WSL2.
set -uo pipefail
export PATH=$PATH:/usr/local/go/bin
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv}
PORT=${PORT:-7423}
N=${N:-8000000}
RAW=${RAW:-/root/f3gate/rep_headroom}
mkdir -p "$RAW"

wait_ping() { for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }

# dual WORKLOAD TAG -> summed ops/s across the two pinned generators
dual() {
  local wl=$1 tag=$2
  taskset -c 18-24 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c 25-31 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!
  wait "$p1"; wait "$p2"
  local ra rb
  ra=$(sed -n 2p "$RAW/$tag-a.csv" | cut -d, -f2 | tr -d '"')
  rb=$(sed -n 2p "$RAW/$tag-b.csv" | cut -d, -f2 | tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f\n", a+b}'
}

hwm() { awk '/VmHWM/{print $2}' "/proc/$1/status"; }

# arm REPCAP -> boots the reactor at the gate flags with -rep-cap REPCAP, runs
# SET (2 reps), reads the write-heavy peak VmHWM, then GET (2 reps).
arm() {
  local rc=$1
  GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr "127.0.0.1:$PORT" -shards 8 \
    -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 \
    -rep-cap "$rc" -net reactor -net-loops 0 >"$RAW/srv-$rc.log" 2>&1 &
  local spid=$!
  wait_ping || { echo "rep-cap=$rc PING FAIL"; kill "$spid" 2>/dev/null; return; }
  grep -qi 'notice\|fall' "$RAW/srv-$rc.log" && echo "  DRIVER FALLBACK!"
  "$CLI" -p "$PORT" flushall >/dev/null
  dual set "w-$rc" >/dev/null
  local s1 s2 setwm g1 g2
  s1=$(dual set "s1-$rc")
  s2=$(dual set "s2-$rc")
  setwm=$(hwm "$spid")   # write-heavy peak: SET replies never grow the node
  dual get "wg-$rc" >/dev/null
  g1=$(dual get "g1-$rc")
  g2=$(dual get "g2-$rc")
  echo "rep-cap=$rc  SET: $s1 $s2   GET: $g1 $g2   SET-cell VmHWM=${setwm}kB"
  kill "$spid" 2>/dev/null; wait "$spid" 2>/dev/null
}

echo "== rep-cap sweep, gate config (server 4-17 gmp14 shards8, gens 18-24 + 25-31) =="
# 0 = tuning.go default (batchDataCap+64*batchCap = 1024+2048 = 3072 at these flags).
for RC in 0 2048 1024 512; do arm "$RC"; done
