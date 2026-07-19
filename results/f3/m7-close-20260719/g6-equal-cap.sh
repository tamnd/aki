#!/bin/bash
# M7-G6 equal-cap arm re-run on the f3 tip: the coverage differential at an equal
# memory budget. Give all three the SAME RAM budget (aki's measured equal-data
# peak, so the rivals get at least as much memory as aki used) and load the same
# 1M keys of 1032-byte values. aki carries a per-shard resident cap plus a value
# log, so cold values spill to disk and it keeps 100% of the keys; the rivals run
# allkeys-lru at the same budget, so they evict what will not fit. The metric is
# coverage: how many of the 1M keys each store can still answer, at equal memory.
# Fair-proof rule honored: no redis-benchmark, one identical loader stream, the
# numbers are coverage (dbsize + a sampled hit rate) and peak VmHWM, not raw ops.
set -uo pipefail
BIN=/root/bin/f3srv
CLI=/root/bin/redis-cli
PORT=7436
M=1000000
V=1032
CAPMB=354   # aki's measured equal-data peak; the fair equal budget for the rivals
export PATH=$PATH:/usr/local/go/bin

VAL=$(head -c $V /dev/zero | tr '\0' 'x')
wait_ping(){ for _ in $(seq 1 150); do "$CLI" -p $PORT ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
hwm_kb(){ awk '/VmHWM/{print $2}' /proc/$1/status 2>/dev/null; }
gen(){ awk -v m=$M -v val="$VAL" 'BEGIN{for(i=0;i<m;i++) printf "SET key:%d %s\r\n", i, val}'; }

start(){
  local t=$1 dir=/root/f3gate/tmp/g6cap-$t; rm -rf "$dir"; mkdir -p "$dir"
  case "$t" in
    aki)    GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 64 -vlog-dir "$dir" -resident-cap-mib 32 -net reactor -net-loops 0 >/tmp/g6cap-$t.log 2>&1 & ;;
    redis)  taskset -c 4-17 /root/bin/redis-server  --port $PORT --bind 127.0.0.1 --save '' --appendonly no --dir "$dir" --maxmemory ${CAPMB}mb --maxmemory-policy allkeys-lru >/tmp/g6cap-$t.log 2>&1 & ;;
    valkey) taskset -c 4-17 /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --dir "$dir" --maxmemory ${CAPMB}mb --maxmemory-policy allkeys-lru >/tmp/g6cap-$t.log 2>&1 & ;;
  esac
  SP=$!; wait_ping
}

declare -A PEAK COV HIT
for t in aki redis valkey; do
  pkill -f 'f3srv -addr' 2>/dev/null || true; pkill -f 'redis-server --port' 2>/dev/null || true; pkill -f 'valkey-server --port' 2>/dev/null || true; sleep 1
  start "$t" || { echo "$t failed to start"; continue; }
  "$CLI" -p $PORT flushall >/dev/null 2>&1
  gen | "$CLI" -p $PORT --pipe >/tmp/g6cap-$t-load.txt 2>&1
  present=$("$CLI" -p $PORT dbsize 2>/dev/null)
  # data-bearing coverage: sample 2000 keys uniformly, count how many still answer
  hits=$(awk -v m=$M 'BEGIN{for(i=0;i<2000;i++) print "key:" int(i*(m/2000))}' | while read k; do "$CLI" -p $PORT exists "$k"; done | awk '{s+=$1} END{print s+0}')
  peak=$(hwm_kb $SP)
  PEAK[$t]=$peak; COV[$t]=$present; HIT[$t]=$hits
  printf "  %-7s dbsize=%-8s sampled_present=%d/2000 (%.1f%%) peakVmHWM=%s kB (%.0f MiB)\n" "$t" "$present" "$hits" "$(awk "BEGIN{print $hits/20}")" "$peak" "$(awk "BEGIN{print $peak/1024}")"
  kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
done
echo
echo "EQUAL-CAP BUDGET: ${CAPMB} MiB for all three (aki's equal-data peak)"
echo "COVERAGE: aki=${COV[aki]:-NA} redis=${COV[redis]:-NA} valkey=${COV[valkey]:-NA} (of $M)"
echo "SAMPLED PRESENT /2000: aki=${HIT[aki]:-NA} redis=${HIT[redis]:-NA} valkey=${HIT[valkey]:-NA}"
akic=${COV[aki]:-0}; rc=${COV[redis]:-1}; vc=${COV[valkey]:-1}
echo "COVERAGE RATIO: aki/redis=$(awk "BEGIN{printf \"%.2f\", $akic/$rc}")x  aki/valkey=$(awk "BEGIN{printf \"%.2f\", $akic/$vc}")x (higher is better: more keys kept at equal memory)"
echo "PEAK: aki=${PEAK[aki]:-NA} redis=${PEAK[redis]:-NA} valkey=${PEAK[valkey]:-NA} kB"
echo G6CAPDONE
