#!/bin/bash
# Reactor 2x gate at the re-swept loop default: reactor (net-loops 0 -> 7) vs
# CF16-frozen rivals, SET/GET 64B P16 c256x2, dual generators, warm + 3 reps.
# The exact driver behind results/f3/reactor-loop-knee-20260719/summary.tsv.
set -uo pipefail
export PATH=$PATH:/usr/local/go/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv}
PORT=${PORT:-7431}; N=${N:-8000000}
SMASK=4-17; C1=18-24; C2=25-31
RAW=${RAW:-/root/f3gate/loopfix-gate}; mkdir -p "$RAW"

wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
dual(){ local wl=$1 tag=$2
  taskset -c $C1 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
start(){ case $1 in
  reactor) GOMAXPROCS=14 taskset -c $SMASK "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0 >"$RAW/$1.log" 2>&1 & ;;
  redis) taskset -c $SMASK /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
  valkey) taskset -c $SMASK /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
 esac; SPID=$!; wait_ping; }

for t in reactor redis valkey; do
  start "$t"
  "$CLI" -p $PORT flushall >/dev/null
  dual set "$t-ws" >/dev/null
  for r in 1 2 3; do echo "$t set rep$r $(dual set "$t-s$r")"; done
  taskset -c $C1 "$RB" -p $PORT -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  dual get "$t-wg" >/dev/null
  for r in 1 2 3; do echo "$t get rep$r $(dual get "$t-g$r")"; done
  awk '/VmHWM/{print "'"$t"' VmHWM "$2" kB"}' /proc/$SPID/status
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null
done
