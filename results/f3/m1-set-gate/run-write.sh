#!/bin/bash
# M1 write point-ops via dual-generator redis-benchmark, custom command so the
# KEY carries __rand_int__ = 1M distinct collections spread across 8 shards,
# not the native single-hot-set. Mirrors m0gate.sh. SADD is the write mirror of
# the passing SISMEMBER read.
set -uo pipefail
export PATH=$PATH:/usr/bin:/usr/local/go/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=/root/bin/f3srv-dcef1d8c
PORT=7461; N=8000000
SMASK=4-17; C1=18-24; C2=25-31
RAW=/root/f3gate/m1-write; mkdir -p "$RAW"
echo "== M1 write dual-gen == $(date -u)"
cleanup(){ pkill -f "f3srv-dcef1d8c|redis-server --port|valkey-server --port" 2>/dev/null; sleep 1; }
cleanup
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
median(){ printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print a[2]}'; }
# dual WL: pass the full custom command as remaining args
dualcmd(){ local tag=$1; shift
  taskset -c $C1 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv "$@" >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv "$@" >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
start(){ case $1 in
  aki) GOGC=20 GOMAXPROCS=14 taskset -c $SMASK "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0 >"$RAW/$1.log" 2>&1 & ;;
  redis) taskset -c $SMASK /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
  valkey) taskset -c $SMASK /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
 esac; SPID=$!; wait_ping; }
proc_kb(){ awk -v k="$1" '$1==k":"{print $2}' /proc/$SPID/status; }

echo "cmd,target,rep1,rep2,rep3,median,vmhwm_kb" > "$RAW/summary.csv"
for t in aki redis valkey; do
  start "$t"
  "$CLI" -p $PORT flushall >/dev/null
  dualcmd "$t-ws" SADD s:__rand_int__ m >/dev/null
  declare -a S=(); for r in 1 2 3; do v=$(dualcmd "$t-s$r" SADD s:__rand_int__ m); S+=("$v"); echo "  $t sadd rep$r $v"; done
  SMED=$(median "${S[@]}")
  HWM=$(proc_kb VmHWM)
  echo "  $t SADD_med=$SMED vmhwm=${HWM}kB"
  echo "sadd,$t,${S[0]},${S[1]},${S[2]},$SMED,$HWM" >> "$RAW/summary.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; sleep 6
  unset S
done
echo "== DONE $(date -u) =="; column -t -s, "$RAW/summary.csv"
