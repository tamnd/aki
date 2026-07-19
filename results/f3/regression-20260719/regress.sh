#!/bin/bash
# Regression confirmation: the headline SET/GET (M8-R1) and collection point reads
# SISMEMBER/ZSCORE/HGET (M9-R1) must stay green on the current tip after the M8
# durable-append / .aki arc, M9 lazy-expiry + OBJECT IDLETIME clock, and M11 box-free
# closure landed. Dual-generator 1M-key protocol, modelled on ptdual.sh. Also dumps
# the reactor loop-knee resolution from the f3srv startup log (M10-R1).
set -uo pipefail
BIN=/root/bin/f3srv
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
PORT=7435
N=3000000
export PATH=$PATH:/usr/local/go/bin
wait_ping(){ for _ in $(seq 1 100); do "$CLI" -p $PORT ping >/dev/null 2>&1 && return 0; sleep 0.1; done; return 1; }
start(){
  local t=$1 dir=/root/f3gate/tmp/rg-$t; rm -rf "$dir"; mkdir -p "$dir"
  case "$t" in
    aki) GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$BIN" -addr 127.0.0.1:$PORT -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0 >/tmp/rg-$t.log 2>&1 & ;;
    redis) taskset -c 4-17 /root/bin/redis-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 --dir "$dir" >/tmp/rg-$t.log 2>&1 & ;;
    valkey) taskset -c 4-17 /root/bin/valkey-server --port $PORT --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes --dir "$dir" >/tmp/rg-$t.log 2>&1 & ;;
  esac
  SP=$!; wait_ping
}
dual(){
  local tag=$1; shift
  taskset -c 18-24 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/rg-$tag-a.csv 2>/dev/null &
  local p1=$!
  taskset -c 25-31 "$RB" -p $PORT -r 1000000 -n $N -c 256 -P 16 --threads 7 --csv "$@" >/tmp/rg-$tag-b.csv 2>/dev/null &
  local p2=$!
  wait $p1; wait $p2
  local ra=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/rg-$tag-a.csv)
  local rb=$(awk -F, 'NR==2{gsub(/"/,"",$2);print $2}' /tmp/rg-$tag-b.csv)
  echo "$ra + $rb" | bc
}
for TYPE in set get sismember zscore hget; do
  case "$TYPE" in
    set)       PRE=(); RD=(SET "key:__rand_int__" "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");;
    get)       PRE=(SET "key:__rand_int__" "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"); RD=(GET "key:__rand_int__");;
    sismember) PRE=(SADD "set:__rand_int__" m); RD=(SISMEMBER "set:__rand_int__" m);;
    zscore)    PRE=(ZADD "zset:__rand_int__" 1 m); RD=(ZSCORE "zset:__rand_int__" m);;
    hget)      PRE=(HSET "hash:__rand_int__" f v); RD=(HGET "hash:__rand_int__" f);;
  esac
  echo "== $TYPE (${RD[*]}), dual-gen c512/P16 1M-key, warm+2 =="
  declare -A R
  for t in aki redis valkey; do
    pkill -f 'f3srv -addr' 2>/dev/null || true; pkill -f 'redis-server --port' 2>/dev/null || true; pkill -f 'valkey-server --port' 2>/dev/null || true; sleep 1
    start "$t"
    "$CLI" -p $PORT flushall >/dev/null
    [ "${#PRE[@]}" -gt 0 ] && dual "pre-$t-$TYPE" "${PRE[@]}" >/dev/null
    best=0
    for rep in warm 1 2; do
      v=$(dual "$t-$TYPE-$rep" "${RD[@]}")
      [ "$rep" != warm ] && awk "BEGIN{exit !($v>$best)}" && best=$v
    done
    R[$t]=$best
    printf "  %-7s rate=%-12s\n" "$t" "$best"
    [ "$t" = aki ] && echo "  LOOPKNEE: $(grep -i 'loop\|net-loops\|reactor' /tmp/rg-aki.log | head -2 | tr '\n' ' ')"
    kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
  done
  ra=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[redis]}}")
  va=$(awk "BEGIN{printf \"%.2f\", ${R[aki]}/${R[valkey]}}")
  echo "  RATIO $TYPE: aki/redis=${ra}x  aki/valkey=${va}x"
done
echo ALLDONE
