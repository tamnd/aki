#!/bin/bash
# Lab 26: the reactor loop-count knee, re-swept on the current surface.
#
# Lab 19 first froze the reactor's default loop count at the 2/5 network share
# of the doc 03 core split (GOMAXPROCS*2/5), off a sweep on the gate box's 8-cpu
# server mask on the pre-M10 surface (commit c76d6c0). This lab re-runs the
# sweep on the current surface, at the real gate config (GOMAXPROCS 14, 8
# shards) and at an 8-cpu control, to check whether the knee still sits where
# lab 19 left it. It does not: the M10 pull-forward (batched owner-to-loop wakes,
# per-loop buffer leasing) flattened the oversubscription penalty a loop past
# the knee used to pay, and the knee moves up to half the cores at both counts.
#
# The measurement is the dual-generator SET/GET gate: two redis-benchmark procs
# beat the single-generator random-key cap. Summed ops/s, warm + 2 reps per
# cell. Run it on the GamingPC gate box under WSL2; it is loop-count-internal
# (one knob, one harness), so no rival ratios are computed here, the slice's
# results/ run pairs it against the CF16-frozen rivals.
set -uo pipefail
export PATH=$PATH:/usr/local/go/bin
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv}
PORT=${PORT:-7422}
N=${N:-8000000}
RAW=${RAW:-/root/f3gate/loop_knee}
mkdir -p "$RAW"

wait_ping() { for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }

# dual WORKLOAD TAG CMASK1 CMASK2 -> summed ops/s across the two generators
dual() {
  local wl=$1 tag=$2 c1=$3 c2=$4
  taskset -c "$c1" "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c "$c2" "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!
  wait "$p1"; wait "$p2"
  local ra rb
  ra=$(sed -n 2p "$RAW/$tag-a.csv" | cut -d, -f2 | tr -d '"')
  rb=$(sed -n 2p "$RAW/$tag-b.csv" | cut -d, -f2 | tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f\n", a+b}'
}

# arm GOMAXPROCS SMASK SHARDS LOOPS CMASK1 CMASK2
arm() {
  local gmp=$1 smask=$2 shards=$3 loops=$4 c1=$5 c2=$6
  GOMAXPROCS="$gmp" taskset -c "$smask" "$BIN" -addr "127.0.0.1:$PORT" -shards "$shards" \
    -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 \
    -net reactor -net-loops "$loops" >"$RAW/srv-$gmp-$shards-$loops.log" 2>&1 &
  local spid=$!
  wait_ping || { echo "gmp=$gmp shards=$shards loops=$loops PING FAIL"; kill "$spid" 2>/dev/null; return; }
  grep -qi 'notice\|fall' "$RAW/srv-$gmp-$shards-$loops.log" && echo "  DRIVER FALLBACK!"
  "$CLI" -p "$PORT" flushall >/dev/null
  dual set "w-$gmp-$shards-$loops" "$c1" "$c2" >/dev/null
  local s1 s2 g1 g2 hwm
  s1=$(dual set "s1-$gmp-$shards-$loops" "$c1" "$c2")
  s2=$(dual set "s2-$gmp-$shards-$loops" "$c1" "$c2")
  taskset -c "$c1" "$RB" -p "$PORT" -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  dual get "wg-$gmp-$shards-$loops" "$c1" "$c2" >/dev/null
  g1=$(dual get "g1-$gmp-$shards-$loops" "$c1" "$c2")
  g2=$(dual get "g2-$gmp-$shards-$loops" "$c1" "$c2")
  hwm=$(awk '/VmHWM/{print $2}' "/proc/$spid/status")
  echo "gmp=$gmp shards=$shards loops=$loops  SET: $s1 $s2   GET: $g1 $g2   VmHWM=${hwm}kB"
  kill "$spid" 2>/dev/null; wait "$spid" 2>/dev/null
}

echo "== 14-cpu gate config (server 4-17, generators 18-24 + 25-31) =="
for L in 4 5 6 7; do arm 14 4-17 8 "$L" 18-24 25-31; done

echo "== 8-cpu control (server 4-11, generators 12-18 + 19-25) =="
for L in 3 4 5; do arm 8 4-11 4 "$L" 12-18 19-25; done
