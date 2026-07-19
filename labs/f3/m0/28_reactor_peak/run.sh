#!/bin/bash
# M0 lab 28: attribute and attack the reactor c512 string peak (the G3 memory row).
# The tip gate reads reactor VmHWM 208 MiB vs redis 164 / valkey 135 at the 64B
# 1M-key P16 c512 cell, while aki used_memory (live data) 113 MiB is the leanest
# of the three. So the peak overage is not the data, it is heap fabric + GC
# headroom on top of the off-heap arena. This lab sweeps the two suspected levers
# (Go GC growth headroom via GOGC/GOMEMLIMIT, and the per-node reply buffer via
# -rep-cap) at the frozen gate flags, measuring VmHWM and throughput per arm, to
# find a config that pulls the peak under the worse rival at flat throughput, or
# to attribute the residual as structural conn-fabric.
set -uo pipefail
export PATH=$PATH:/usr/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv-dcef1d8c}
PORT=${PORT:-7433}; N=${N:-8000000}
SMASK=4-17; C1=18-24; C2=25-31
RAW=${RAW:-/root/f3gate/m0-lab28}; mkdir -p "$RAW"
echo "== lab28 reactor peak attribution == BIN=$BIN $(date -u)"

wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
median2(){ printf '%s\n%s\n' "$1" "$2" | sort -n | tail -1; }  # max of 2 = optimistic throughput
dual(){ local wl=$1 tag=$2
  taskset -c $C1 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
usedmem(){ "$CLI" -p $PORT info memory 2>/dev/null | awk -F: '/^used_memory:/{gsub(/\r/,"");print $2}'; }
kb(){ awk -v k="$1" '$1==k":"{print $2}' /proc/$SPID/status; }

# arm NAME "ENVPREFIX" "EXTRAFLAGS"
arm(){ local name=$1 envp=$2 extra=$3
  eval "GOMAXPROCS=14 $envp taskset -c $SMASK $BIN -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0 $extra >$RAW/$name.log 2>&1 &"
  SPID=$!; wait_ping || { echo "$name LAUNCH FAIL"; tail -3 $RAW/$name.log; return 1; }
  "$CLI" -p $PORT flushall >/dev/null
  dual set "$name-ws" >/dev/null
  local s1 s2; s1=$(dual set "$name-s1"); s2=$(dual set "$name-s2"); local smax=$(median2 $s1 $s2)
  taskset -c $C1 "$RB" -p $PORT -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  local hwm_set=$(kb VmHWM)
  dual get "$name-wg" >/dev/null
  local g1 g2; g1=$(dual get "$name-g1"); g2=$(dual get "$name-g2"); local gmax=$(median2 $g1 $g2)
  local hwm=$(kb VmHWM); local rss=$(kb VmRSS); local um=$(usedmem)
  printf "%-16s SET=%-9s GET=%-9s hwm_afterset=%-8s vmhwm=%-8s vmrss=%-8s used=%s\n" "$name" "$smax" "$gmax" "$hwm_set" "$hwm" "$rss" "$um"
  echo "$name,$smax,$gmax,$hwm_set,$hwm,$rss,$um" >> "$RAW/lab28.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; sleep 6
}

echo "arm,set,get,vmhwm_afterset_kb,vmhwm_kb,vmrss_kb,used_memory" > "$RAW/lab28.csv"
arm baseline        ""                    ""
arm repcap512       ""                    "-rep-cap 512"
arm gogc50          "GOGC=50"             ""
arm gogc50_rep512   "GOGC=50"             "-rep-cap 512"
arm gogc30_rep512   "GOGC=30"             "-rep-cap 512"
arm memlimit180     "GOMEMLIMIT=180MiB"   "-rep-cap 512"
echo "== DONE $(date -u) =="; echo "rival ref: redis vmhwm 164548 kB, valkey vmhwm 134832 kB (this box, this harness)"; column -t -s, "$RAW/lab28.csv"
