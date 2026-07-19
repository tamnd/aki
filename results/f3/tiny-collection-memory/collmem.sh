#!/usr/bin/env bash
# Tiny-collection memory: N small collections of each type, VmHWM peak per engine.
# Fresh server set per type so memory is attributable. Drives ZADD/RPUSH/HSET/XADD
# through redis-cli --pipe, reads /proc/PID/status VmHWM.
set -u
CLI=/root/bin/redis-cli
N=${N:-100000}
M=${M:-8}
run_type() {
  local typ=$1
  pkill -9 -f 'f3sr[v]|redis-serve[r]|valkey-serve[r]' 2>/dev/null; sleep 1
  GOGC=20 GOMAXPROCS=14 taskset -c 4-17 /root/bin/f3srv -addr 127.0.0.1:7001 -shards 8 \
    -arena-mib 512 -net reactor -net-loops 0 </dev/null >/tmp/c1.log 2>&1 & local pA=$!
  rm -rf /tmp/rd; mkdir -p /tmp/rd
  taskset -c 4-17 /root/bin/redis-server --port 7002 --dir /tmp/rd --save '' --appendonly no </dev/null >/tmp/c2.log 2>&1 & local pR=$!
  rm -rf /tmp/vd; mkdir -p /tmp/vd
  taskset -c 4-17 /root/bin/valkey-server --port 7003 --dir /tmp/vd --save '' --appendonly no </dev/null >/tmp/c3.log 2>&1 & local pV=$!
  for p in 7001 7002 7003; do for _ in $(seq 1 50); do [ "$($CLI -p $p ping 2>/dev/null)" = PONG ] && break; sleep 0.2; done; done
  local F=/tmp/coll_$typ.txt; : > $F
  local k
  for ((k=0;k<N;k++)); do
    case $typ in
      zset) printf 'ZADD z:%d' $k >> $F; for ((e=0;e<M;e++)); do printf ' %d m%d' $e $e >> $F; done; printf '\n' >> $F;;
      list) printf 'RPUSH l:%d' $k >> $F; for ((e=0;e<M;e++)); do printf ' m%d' $e >> $F; done; printf '\n' >> $F;;
      hash) printf 'HSET h:%d' $k >> $F; for ((e=0;e<M;e++)); do printf ' f%d v%d' $e $e >> $F; done; printf '\n' >> $F;;
      stream) local e; for ((e=0;e<M;e++)); do printf 'XADD s:%d * f v%d\n' $k $e >> $F; done;;
    esac
  done
  for p in 7001 7002 7003; do $CLI -p $p --pipe < $F >/dev/null 2>&1; done
  sleep 1
  local a r v
  a=$(awk '/VmHWM/{print $2}' /proc/$pA/status)
  r=$(awk '/VmHWM/{print $2}' /proc/$pR/status)
  v=$(awk '/VmHWM/{print $2}' /proc/$pV/status)
  awk "BEGIN{printf \"%-7s N=$N M=$M  aki=%d kB  redis=%d kB  valkey=%d kB  aki/redis=%.3f  aki/valkey=%.3f\n\", \"$typ\", $a, $r, $v, $a/$r, $a/$v}"
  kill $pA $pR $pV 2>/dev/null; wait 2>/dev/null
}
for t in zset list hash stream; do run_type $t; done
echo COLL_DONE
