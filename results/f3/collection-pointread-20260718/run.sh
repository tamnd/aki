#!/bin/bash
# Collection point-READ gate under the M0 dual-generator 1M-key protocol.
# Tests whether the stale M1/M2/M4 point-op "misses" (1.3-1.5x) were a
# wrong-cell/client-cap artifact like M0's were. Each type: preload 1M distinct
# keys spread across shards, then measure the point read with two summed -r
# generators. aki-goroutine vs CF16-frozen redis io=6 / valkey io=4.
set -euo pipefail
TYPE=$1   # set | hash | zset
BIN=/root/f3gate/m0-driver-20260718/bin/f3srv
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
PORT=7411
N=6000000
export PATH=$PATH:/usr/local/go/bin
case "$TYPE" in
  set)  PRE=(SADD "set:__rand_int__" hello);  RD=(SISMEMBER "set:__rand_int__" hello);;
  hash) PRE=(HSET "hash:__rand_int__" f v);   RD=(HGET "hash:__rand_int__" f);;
  zset) PRE=(ZADD "zset:__rand_int__" 1 m);   RD=(ZSCORE "zset:__rand_int__" m);;
esac
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p $PORT ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
start(){
  local t=$1 dir=/root/f3gate/tmp/$t-$TYPE; rm -rf "$dir"; mkdir -p "$dir"
  case "$t" in
    aki) GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 >/tmp/cp-$t-$TYPE.log 2>&1 &;;
    redis) taskset -c 4-17 /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir "$dir" >/tmp/cp-$t-$TYPE.log 2>&1 &;;
    valkey) taskset -c 4-17 /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir "$dir" >/tmp/cp-$t-$TYPE.log 2>&1 &;;
  esac
  SP=$!; wait_ping
}
dual(){ # cmd... -> echoes summed ops/s
  local tag=$1; shift
  taskset -c 18-24 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/cp-$tag-a.csv 2>/dev/null &
  local p1=$!
  taskset -c 25-31 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/cp-$tag-b.csv 2>/dev/null &
  local p2=$!
  wait $p1; wait $p2
  local ra=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/cp-$tag-a.csv)
  local rb=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/cp-$tag-b.csv)
  echo "$ra + $rb" | bc
}
echo "== $TYPE point-read gate (${RD[*]}), dual-gen c512/P16 1M-key, warm+2 =="
declare -A R
for t in aki redis valkey; do
  pkill -f 'f3srv -addr' 2>/dev/null || true; pkill -f 'redis-server --port' 2>/dev/null || true; pkill -f 'valkey-server --port' 2>/dev/null || true; sleep 1
  start "$t"
  "$CLI" -p $PORT flushall >/dev/null
  dual "pre-$t" "${PRE[@]}" >/dev/null   # preload 1M keys
  local_db=$("$CLI" -p $PORT dbsize)
  best=0
  for rep in warm 1 2; do
    v=$(dual "$t-$TYPE-$rep" "${RD[@]}")
    [ "$rep" != warm ] && awk "BEGIN{exit !($v>$best)}" && best=$v
  done
  hwm=$(awk '/VmHWM/{print $2}' /proc/$SP/status)
  R[$t]=$best
  printf "  %-7s read=%-12s dbsize=%s VmHWM=%skB\n" "$t" "$best" "$local_db" "$hwm"
  kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
done
ra=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[redis]}}")
va=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[valkey]}}")
echo "  RATIO $TYPE: aki/redis=${ra}x  aki/valkey=${va}x  (min=$(awk "BEGIN{m=${ra}<${va}?${ra}:${va}; printf \"%.2f\", m}")x)"
pkill -f 'f3srv -addr' 2>/dev/null || true
