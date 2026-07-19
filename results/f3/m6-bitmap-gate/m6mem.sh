#!/usr/bin/env bash
set -u
CLI=/root/bin/redis-cli
GOGC=20 GOMAXPROCS=14 taskset -c 4-17 /root/bin/f3srv -addr 127.0.0.1:7001 -shards 8 \
  -arena-mib 512 -net reactor -net-loops 0 </dev/null >/tmp/m1.log 2>&1 & pA=$!
rm -rf /tmp/rd; mkdir -p /tmp/rd
taskset -c 4-17 /root/bin/redis-server --port 7002 --dir /tmp/rd --save '' --appendonly no </dev/null >/tmp/m2.log 2>&1 & pR=$!
rm -rf /tmp/vd; mkdir -p /tmp/vd
taskset -c 4-17 /root/bin/valkey-server --port 7003 --dir /tmp/vd --save '' --appendonly no </dev/null >/tmp/m3.log 2>&1 & pV=$!
for p in 7001 7002 7003; do for _ in $(seq 1 50); do [ "$($CLI -p $p ping 2>/dev/null)" = PONG ] && break; sleep 0.2; done; done

hwm() { awk '/VmHWM/{print $2}' /proc/$1/status; }
base_a=$(hwm $pA); base_r=$(hwm $pR); base_v=$(hwm $pV)

# build one RESP command stream: 50 sparse bitmaps (6 bits over 1Gbit) + 20 HLLs
CMDS=/tmp/m6cmds.txt; : > $CMDS
for k in $(seq 1 50); do
  for off in 0 200000000 400000000 600000000 800000000 999999999; do
    printf 'SETBIT bm:%s %s 1\n' "$k" "$off" >> $CMDS
  done
done
for k in $(seq 1 20); do
  printf 'PFADD hll:%s' "$k" >> $CMDS
  for e in $(seq 1 1000); do printf ' m%s' "$e" >> $CMDS; done
  printf '\n' >> $CMDS
done

for p in 7001 7002 7003; do $CLI -p $p --pipe < $CMDS >/dev/null 2>&1; done
sleep 1
a=$(hwm $pA); r=$(hwm $pR); v=$(hwm $pV)
echo "sparse-bitmap(50 keys x 6 bits over 1Gbit) + 20 HLL x 1000 elems"
echo "VmHWM kB:  aki $a  redis $r  valkey $v"
echo "delta kB:  aki $(( a-base_a ))  redis $(( r-base_r ))  valkey $(( v-base_v ))"
awk "BEGIN{printf \"aki/redis peak = %.4f   aki/valkey peak = %.4f\n\", $a/$r, $a/$v}"
kill $pA $pR $pV 2>/dev/null; wait 2>/dev/null
echo MEM_DONE
