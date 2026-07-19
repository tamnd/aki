#!/bin/bash
# M0 headline gate on the current tip: reactor (net-loops 0 -> 7) vs CF16-frozen
# rivals, SET/GET 64B P16 c256x2 dual generators, warm + 3 reps, median summed.
# Enhanced over reactor-loop-knee run.sh: also captures used_memory (live data)
# and VmRSS alongside VmHWM for the G3 memory row, plus CF2/CF16 readbacks.
set -uo pipefail
export PATH=$PATH:/usr/bin:/usr/local/go/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv-dcef1d8c}
PORT=${PORT:-7431}; N=${N:-8000000}
SMASK=4-17; C1=18-24; C2=25-31
RAW=${RAW:-/root/f3gate/m0-tip-gate}; mkdir -p "$RAW"
echo "== M0 tip gate == BIN=$BIN commit=$(basename $BIN) $(date -u)"

wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
median(){ printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print a[2]}'; }
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
usedmem(){ "$CLI" -p $PORT info memory 2>/dev/null | awk -F: '/^used_memory:/{gsub(/\r/,"");print $2}'; }
proc_kb(){ awk -v k="$1" '$1==k":"{print $2}' /proc/$SPID/status; }

echo "commit,target,workload,rep1,rep2,rep3,median,used_memory,vmhwm_kb,vmrss_kb,pin,iothreads" > "$RAW/summary.csv"
for t in reactor redis valkey; do
  start "$t"
  # CF2 pin readback + CF16 io-threads readback
  PIN=$(taskset -cp $SPID 2>/dev/null | awk '{print $NF}')
  IOT=$(tr '\0' ' ' </proc/$SPID/cmdline | grep -oE 'io-threads [0-9]+' | head -1 | awk '{print $2}')
  [ "$t" = reactor ] && IOT="net-loops7"
  "$CLI" -p $PORT flushall >/dev/null
  dual set "$t-ws" >/dev/null
  declare -a S=(); for r in 1 2 3; do v=$(dual set "$t-s$r"); S+=("$v"); echo "  $t set rep$r $v"; done
  SMED=$(median "${S[@]}")
  # prime keyspace for GET
  taskset -c $C1 "$RB" -p $PORT -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  UM_SET=$(usedmem)
  dual get "$t-wg" >/dev/null
  declare -a G=(); for r in 1 2 3; do v=$(dual get "$t-g$r"); G+=("$v"); echo "  $t get rep$r $v"; done
  GMED=$(median "${G[@]}")
  HWM=$(proc_kb VmHWM); RSS=$(proc_kb VmRSS); UM=$(usedmem)
  echo "  $t SET_med=$SMED GET_med=$GMED used_memory=$UM vmhwm=${HWM}kB vmrss=${RSS}kB pin=$PIN iot=$IOT"
  echo "$(basename $BIN),$t,set,${S[0]},${S[1]},${S[2]},$SMED,$UM,$HWM,$RSS,$PIN,$IOT" >> "$RAW/summary.csv"
  echo "$(basename $BIN),$t,get,${G[0]},${G[1]},${G[2]},$GMED,$UM,$HWM,$RSS,$PIN,$IOT" >> "$RAW/summary.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null
  sleep 6   # O13 cooldown between servers
  unset S G
done
echo "== DONE $(date -u) =="; cat "$RAW/summary.csv"
