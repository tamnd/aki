#!/bin/bash
# M0-G10 fan-out gate: MSET (builtin -t mset, 10-key cross-shard fan) and MGET
# (16-key custom command over the primed 1M keyspace) for reactor vs CF16-frozen
# rivals. Dual generators, warm + 3 reps, median of summed throughput, VmHWM.
# Validates the fan-out scatter allocation elision (branch m0-mget-fan-scatter-pool).
set -uo pipefail
export PATH=$PATH:/usr/bin:/usr/local/go/bin
RB=/root/bin/redis-benchmark; CLI=/root/bin/redis-cli
BIN=${BIN:-/root/bin/f3srv-m0scatter}
PORT=${PORT:-7433}; N=${N:-3000000}
SMASK=4-17; C1=18-24; C2=25-31
RAW=${RAW:-/root/f3gate/m0-fan-gate}; mkdir -p "$RAW"
echo "== M0-G10 fan gate == BIN=$BIN $(date -u)"

# 16-key MGET custom command over the primed keyspace.
MGETCMD="MGET"
for i in $(seq 1 16); do MGETCMD="$MGETCMD key:__rand_int__"; done

wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
median(){ printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print a[2]}'; }
dual_t(){ local wl=$1 tag=$2
  taskset -c $C1 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -t "$wl" -d 64 -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
dual_c(){ local tag=$1
  taskset -c $C1 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv $MGETCMD >"$RAW/$tag-a.csv" 2>/dev/null &
  local p1=$!
  taskset -c $C2 "$RB" -p "$PORT" -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv $MGETCMD >"$RAW/$tag-b.csv" 2>/dev/null &
  local p2=$!; wait $p1; wait $p2
  local ra rb; ra=$(sed -n 2p "$RAW/$tag-a.csv"|cut -d, -f2|tr -d '"'); rb=$(sed -n 2p "$RAW/$tag-b.csv"|cut -d, -f2|tr -d '"')
  awk -v a="$ra" -v b="$rb" 'BEGIN{printf "%.0f", a+b}'
}
start(){ case $1 in
  reactor) GOMAXPROCS=14 GOGC=20 taskset -c $SMASK "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0 >"$RAW/$1.log" 2>&1 & ;;
  redis) taskset -c $SMASK /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
  valkey) taskset -c $SMASK /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir /tmp >"$RAW/$1.log" 2>&1 & ;;
 esac; SPID=$!; wait_ping; }
proc_kb(){ awk -v k="$1" '$1==k":"{print $2}' /proc/$SPID/status; }

echo "target,workload,rep1,rep2,rep3,median,vmhwm_kb" > "$RAW/summary.csv"
declare -A MSETMED MGETMED
for t in reactor redis valkey; do
  start "$t"
  "$CLI" -p $PORT flushall >/dev/null
  # MSET: builtin, writes 10 keys/command across shards
  dual_t mset "$t-wm" >/dev/null
  declare -a M=(); for r in 1 2 3; do v=$(dual_t mset "$t-m$r"); M+=("$v"); echo "  $t mset rep$r $v"; sleep 4; done
  MM=$(median "${M[@]}"); MSETMED[$t]=$MM
  # prime keyspace for MGET
  taskset -c $C1 "$RB" -p $PORT -t set -d 64 -r 1000000 -n 2500000 -c 256 -P 16 --threads 7 --csv >/dev/null 2>&1
  dual_c "$t-wG" >/dev/null
  declare -a G=(); for r in 1 2 3; do v=$(dual_c "$t-G$r"); G+=("$v"); echo "  $t mget rep$r $v"; sleep 4; done
  GG=$(median "${G[@]}"); MGETMED[$t]=$GG
  HWM=$(proc_kb VmHWM)
  echo "  $t MSET_med=$MM MGET_med=$GG vmhwm=${HWM}kB"
  echo "$t,mset,${M[0]},${M[1]},${M[2]},$MM,$HWM" >> "$RAW/summary.csv"
  echo "$t,mget,${G[0]},${G[1]},${G[2]},$GG,$HWM" >> "$RAW/summary.csv"
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null
  sleep 6
  unset M G
done
echo "== RATIOS =="
awk -v ar="${MSETMED[reactor]}" -v rr="${MSETMED[redis]}" -v vr="${MSETMED[valkey]}" 'BEGIN{printf "MSET reactor=%s  vs redis=%.2fx  vs valkey=%.2fx  gate=min=%.2fx\n", ar, ar/rr, ar/vr, (ar/rr<ar/vr?ar/rr:ar/vr)}'
awk -v ar="${MGETMED[reactor]}" -v rr="${MGETMED[redis]}" -v vr="${MGETMED[valkey]}" 'BEGIN{printf "MGET reactor=%s  vs redis=%.2fx  vs valkey=%.2fx  gate=min=%.2fx\n", ar, ar/rr, ar/vr, (ar/rr<ar/vr?ar/rr:ar/vr)}'
echo "== DONE $(date -u) =="; cat "$RAW/summary.csv"
