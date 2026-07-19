#!/bin/bash
# String-workload peak memory investigation for the reactor (labs/f3/m9/03).
# Four sweeps: dataset placement, per-connection read/reply buffers, the reorder
# ring plus batch caps, and GOGC. Run on the gate box. BIN points at an f3srv
# build, the redis-benchmark and redis-cli tools are under /root/bin.
set -u
BIN=${BIN:-/root/bin/f3srv}
PORT=${PORT:-7455}
BENCH=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli

start() { # extra-env extra-flags...
  pkill -f "f3srv" 2>/dev/null; sleep 1
  eval "$1 GOMAXPROCS=14 $BIN -shards 8 -arena-mib 512 -addr :$PORT -net reactor -net-loops 0 ${2:-} >/tmp/s.log 2>&1 &"
  PID=$!; sleep 2
  kill -0 $PID 2>/dev/null || { echo "  server DIED"; tail -2 /tmp/s.log; return 1; }
}
gen() { $BENCH -p $PORT -t $1 -r 1000000 -n ${2:-4000000} -d 64 -c ${3:-256} -P 16 --threads 7 -q 2>/dev/null | grep -oE "^[0-9.]+ requests" | grep -oE "^[0-9.]+"; }
hwm() { grep VmHWM /proc/$PID/status | grep -oE "[0-9]+"; }

echo "### 1. dataset placement (gctrace, low conns) ###"
start "GODEBUG=gctrace=1" "-reply-ring 128 -batch-data-cap 1024" || exit 1
$BENCH -p $PORT -t set -r 1000000 -n 1000000 -d 64 -c 50 -q >/dev/null 2>&1
echo "  dbsize=$($CLI -p $PORT dbsize)  VmHWM=$(hwm)kB  goheap:"; grep -E "^gc [0-9]" /tmp/s.log | tail -1
kill $PID 2>/dev/null

echo "### 2. read/reply buffer size (256 conns) ###"
for k in 64 32 16 8 4; do
  start "" "-reply-ring 128 -batch-data-cap 1024 -read-buf-kib $k -reply-buf-kib $k" || continue
  gen set >/dev/null; printf "  buf=%-3sk SET=%-10s GET=%-10s VmHWM=%skB\n" $k $(gen set) $(gen get) $(hwm); kill $PID 2>/dev/null
done

echo "### 3. reorder ring / batch cap (256 conns) ###"
for cfg in "128 1024 8 64" "64 1024 4 32" "32 512 4 16" "32 256 2 16" "24 256 2 8"; do
  set -- $cfg
  start "" "-reply-ring $1 -batch-data-cap $2 -free-list-cap $3 -read-buf-kib $4 -reply-buf-kib $4" || continue
  printf "  ring=%-3s bd=%-4s fl=%-2s buf=%sk SET=%-10s GET=%-10s VmHWM=%skB\n" $1 $2 $3 $4 $(gen set) $(gen get) $(hwm); kill $PID 2>/dev/null
done

echo "### 4. GOGC (512 conns, baseline buffers) ###"
for g in 100 25 10; do
  start "GOGC=$g" "-reply-ring 128 -batch-data-cap 1024 -free-list-cap 8" || continue
  printf "  GOGC=%-4s SET=%-10s GET=%-10s VmHWM=%skB\n" $g $(gen set 6000000 512) $(gen get 6000000 512) $(hwm); kill $PID 2>/dev/null
done
pkill -f "f3srv" 2>/dev/null
