#!/bin/bash
# M1 tiny-set memory attribution: 1M single-member sets, sweep GOGC to split
# GC-headroom slack from the live-heap floor. Collections are on-heap (unlike
# the string arena) so GOGC bites here. Reports VmHWM + used_memory per arm.
set -uo pipefail
export PATH=$PATH:/usr/bin:/usr/local/go/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=/root/bin/f3srv-dcef1d8c
PORT=7471; N=8000000
SMASK=4-17; C1=18-24; C2=25-31
RAW=/root/f3gate/m1-mem; mkdir -p "$RAW"
echo "== M1 tiny-set mem attribution == $(date -u)"
cleanup(){ pkill -f "f3srv-dcef1d8c" 2>/dev/null; sleep 1; }
cleanup
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
dualcmd(){ local tag=$1; shift
  taskset -c $C1 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv "$@" >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv "$@" >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
usedmem(){ "$CLI" -p $PORT info memory 2>/dev/null | awk -F: '/^used_memory:/{gsub(/\r/,"");print $2}'; }
kb(){ awk -v k="$1" '$1==k":"{print $2}' /proc/$SPID/status; }
arm(){ local gogc=$1
  GOGC=$gogc GOMAXPROCS=14 taskset -c $SMASK "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0 >"$RAW/gogc$gogc.log" 2>&1 &
  SPID=$!; wait_ping || { echo "gogc$gogc FAIL"; return 1; }
  "$CLI" -p $PORT flushall >/dev/null
  dualcmd "g$gogc-w" SADD s:__rand_int__ m >/dev/null
  local s1 s2; s1=$(dualcmd "g$gogc-1" SADD s:__rand_int__ m); s2=$(dualcmd "g$gogc-2" SADD s:__rand_int__ m)
  local smax=$(printf '%s\n%s\n' $s1 $s2 | sort -n | tail -1)
  local hwm=$(kb VmHWM); local um=$(usedmem)
  printf "gogc=%-3s SADD=%-9s vmhwm=%-8s used=%s\n" "$gogc" "$smax" "$hwm" "$um"
  echo "$gogc,$smax,$hwm,$um" >> "$RAW/mem.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; sleep 6
}
echo "gogc,sadd,vmhwm_kb,used_memory" > "$RAW/mem.csv"
arm 20
arm 10
arm 5
echo "== DONE $(date -u) =="; echo "rival ref: redis vmhwm 91852 / valkey 92292 kB"; column -t -s, "$RAW/mem.csv"
