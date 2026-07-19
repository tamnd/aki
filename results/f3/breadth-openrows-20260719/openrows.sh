#!/bin/bash
# Open gate rows under the dual-generator 1M-key protocol, modelled on ptdual.sh
# (two summed redis-benchmark generators surface aki's true O(1) rate past the
# single-generator client cap). Reactor gate binary vs CF16-frozen rivals.
# Rows measured directly with redis-benchmark raw commands:
#   M5-G3 XLEN      (O(1) metadata read, stream length counter)
#   M4-G7 HINCRBY   (write-modify, read-modify-write one field)
#   M4-G8 HRANDFIELD(reply row, random draw)
#   M2-G8 ZPOPMIN   (reply-plus-mutate, re-add before each rep like hdel/zrem)
set -uo pipefail
BIN=/root/bin/f3srv
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
PORT=7434
N=3000000
export PATH=$PATH:/usr/local/go/bin
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p $PORT ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
start(){
  local t=$1 dir=/root/f3gate/tmp/or-$t; rm -rf "$dir"; mkdir -p "$dir"
  case "$t" in
    aki) GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0 >/tmp/or-$t.log 2>&1 & ;;
    redis) taskset -c 4-17 /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir "$dir" >/tmp/or-$t.log 2>&1 & ;;
    valkey) taskset -c 4-17 /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir "$dir" >/tmp/or-$t.log 2>&1 & ;;
  esac
  SP=$!; wait_ping
}
dual(){
  local tag=$1; shift
  taskset -c 18-24 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/or-$tag-a.csv 2>/dev/null &
  local p1=$!
  taskset -c 25-31 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/or-$tag-b.csv 2>/dev/null &
  local p2=$!
  wait $p1; wait $p2
  local ra=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/or-$tag-a.csv)
  local rb=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/or-$tag-b.csv)
  echo "$ra + $rb" | bc
}
for TYPE in xlen hincrby hrandfield zpopmin; do
  case "$TYPE" in
    xlen)       PRE=(XADD "stream:__rand_int__" '*' f v); RD=(XLEN "stream:__rand_int__");;
    hincrby)    PRE=(HSET "hash:__rand_int__" f 0);       RD=(HINCRBY "hash:__rand_int__" f 1);;
    hrandfield) PRE=(HSET "hash:__rand_int__" f v);       RD=(HRANDFIELD "hash:__rand_int__");;
    zpopmin)    PRE=(ZADD "zset:__rand_int__" 1 m);       RD=(ZPOPMIN "zset:__rand_int__");;
  esac
  echo "== $TYPE (${RD[*]}), dual-gen c512/P16 1M-key, warm+2 =="
  declare -A R
  for t in aki redis valkey; do
    pkill -f 'f3srv -addr' 2>/dev/null || true; pkill -f 'redis-server --port' 2>/dev/null || true; pkill -f 'valkey-server --port' 2>/dev/null || true; sleep 1
    start "$t"
    "$CLI" -p $PORT flushall >/dev/null
    dual "pre-$t-$TYPE" "${PRE[@]}" >/dev/null
    best=0
    for rep in warm 1 2; do
      # zpopmin drains its single-member zset, so re-add before each rep like hdel/zrem
      case "$TYPE" in zpopmin) dual "readd-$t-$TYPE-$rep" "${PRE[@]}" >/dev/null;; esac
      v=$(dual "$t-$TYPE-$rep" "${RD[@]}")
      [ "$rep" != warm ] && awk "BEGIN{exit !($v>$best)}" && best=$v
    done
    R[$t]=$best
    printf "  %-7s rate=%-12s\n" "$t" "$best"
    kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
  done
  ra=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[redis]}}")
  va=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[valkey]}}")
  echo "  RATIO $TYPE: aki/redis=${ra}x  aki/valkey=${va}x"
done
echo ALLDONE
