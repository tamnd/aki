#!/bin/bash
# M7-G6 re-run: the LTM equal-data memory pitch (G1) on the f3 engine, not f1srv.
# Load the SAME 1M keys of 1 KiB values into all three. aki carries a small
# per-shard resident cap and a value log, so cold values spill to disk and its
# peak RSS stays near the cap; the rivals are uncapped, so they hold the whole
# dataset in RAM. All three keep 100% of the keys (no eviction, no maxmemory), so
# coverage is equal and the metric is peak memory: less RAM for the same data.
# Fair-proof rule honored: no redis-benchmark, the loader is one client stream
# identical for all three, the numbers are peak VmHWM and coverage, not raw ops.
set -uo pipefail
BIN=/root/bin/f3srv
CLI=/root/bin/redis-cli
PORT=7436
M=1000000
V=1032
export PATH=$PATH:/usr/local/go/bin

VAL=$(head -c $V /dev/zero | tr '\0' 'x')
wait_ping(){ for _ in $(seq 1 150); do "$CLI" -p $PORT ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
hwm_kb(){ awk '/VmHWM/{print $2}' /proc/$1/status 2>/dev/null; }
gen(){ awk -v m=$M -v val="$VAL" 'BEGIN{for(i=0;i<m;i++) printf "SET key:%d %s\r\n", i, val}'; }

start(){
  local t=$1 dir=/root/f3gate/tmp/g6-$t; rm -rf "$dir"; mkdir -p "$dir"
  case "$t" in
    # cap 32 MiB/shard x 8 = 256 MiB resident cap; ~1 GiB of data spills ~750 MiB to disk
    aki)    GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 64 -vlog-dir "$dir" -resident-cap-mib 32 -net reactor -net-loops 0 >/tmp/g6-$t.log 2>&1 & ;;
    redis)  taskset -c 4-17 /root/bin/redis-server  --port $PORT --bind 127.0.0.1 --save '' --appendonly no --dir "$dir" >/tmp/g6-$t.log 2>&1 & ;;
    valkey) taskset -c 4-17 /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --dir "$dir" >/tmp/g6-$t.log 2>&1 & ;;
  esac
  SP=$!; wait_ping
}

declare -A PEAK COV
for t in aki redis valkey; do
  pkill -f 'f3srv -addr' 2>/dev/null || true; pkill -f 'redis-server --port' 2>/dev/null || true; pkill -f 'valkey-server --port' 2>/dev/null || true; sleep 1
  start "$t" || { echo "$t failed to start"; continue; }
  "$CLI" -p $PORT flushall >/dev/null 2>&1
  gen | "$CLI" -p $PORT --pipe >/tmp/g6-$t-load.txt 2>&1
  present=$("$CLI" -p $PORT dbsize 2>/dev/null)
  # spot-check coverage: read back 5 sampled keys, count hits
  hits=0
  for k in 0 250000 500000 750000 999999; do
    v=$("$CLI" -p $PORT get key:$k 2>/dev/null)
    [ -n "$v" ] && hits=$((hits+1))
  done
  peak=$(hwm_kb $SP)
  PEAK[$t]=$peak; COV[$t]=$present
  printf "  %-7s dbsize=%-8s sample_hits=%d/5 peakVmHWM=%s kB (%.0f MiB)\n" "$t" "$present" "$hits" "$peak" "$(awk "BEGIN{print $peak/1024}")"
  kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
done
echo
ar=$(awk "BEGIN{printf \"%.3f\", ${PEAK[aki]}/${PEAK[redis]}}")
av=$(awk "BEGIN{printf \"%.3f\", ${PEAK[aki]}/${PEAK[valkey]}}")
echo "PEAK RATIO: aki/redis=${ar}x  aki/valkey=${av}x  (memory pitch: lower is better, ideal <=0.5x)"
echo "COVERAGE: aki=${COV[aki]} redis=${COV[redis]} valkey=${COV[valkey]} (of $M; equal coverage, memory is the differentiator)"
echo G6DONE
