#!/bin/bash
# M0-G3 confirmation: reactor with the lab-28 winning memory config, median-of-3,
# proper VmHWM, vs the two arms that bracket redis/valkey peak. Decides whether
# GOGC=30 -rep-cap 512 holds under redis at the median and how close GOGC=20 gets
# to valkey without a throughput cost.
set -uo pipefail
export PATH=$PATH:/usr/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv-dcef1d8c}
PORT=${PORT:-7434}; N=${N:-8000000}
SMASK=4-17; C1=18-24; C2=25-31
RAW=${RAW:-/root/f3gate/m0-g3-confirm}; mkdir -p "$RAW"
echo "== M0-G3 confirm == $(date -u)"
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
med3(){ printf '%s\n%s\n%s\n' "$1" "$2" "$3" | sort -n | sed -n 2p; }
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
arm(){ local name=$1 envp=$2 extra=$3
  eval "GOMAXPROCS=14 $envp taskset -c $SMASK $BIN -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0 $extra >$RAW/$name.log 2>&1 &"
  SPID=$!; wait_ping || { echo "$name FAIL"; tail -3 $RAW/$name.log; return 1; }
  "$CLI" -p $PORT flushall >/dev/null
  dual set "$name-ws" >/dev/null
  local s1 s2 s3; s1=$(dual set "$name-s1"); s2=$(dual set "$name-s2"); s3=$(dual set "$name-s3")
  taskset -c $C1 "$RB" -p $PORT -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  dual get "$name-wg" >/dev/null
  local g1 g2 g3; g1=$(dual get "$name-g1"); g2=$(dual get "$name-g2"); g3=$(dual get "$name-g3")
  local sm=$(med3 $s1 $s2 $s3); local gm=$(med3 $g1 $g2 $g3)
  local hwm=$(kb VmHWM); local rss=$(kb VmRSS); local um=$(usedmem)
  printf "%-16s SET_med=%-9s GET_med=%-9s vmhwm=%-8s vmrss=%-8s used=%s\n" "$name" "$sm" "$gm" "$hwm" "$rss" "$um"
  echo "$name,$sm,$gm,$hwm,$rss,$um" >> "$RAW/confirm.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; sleep 6
}
echo "arm,set_med,get_med,vmhwm_kb,vmrss_kb,used_memory" > "$RAW/confirm.csv"
arm gogc30_rep512  "GOGC=30"  "-rep-cap 512"
arm gogc20_rep512  "GOGC=20"  "-rep-cap 512"
arm gogc30_rep256  "GOGC=30"  "-rep-cap 256"
echo "== DONE $(date -u) =="
echo "rival ref this box/harness: redis vmhwm 164548 kB / valkey vmhwm 134832 kB"
column -t -s, "$RAW/confirm.csv"
