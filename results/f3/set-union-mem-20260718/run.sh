#!/bin/bash
# Same-box A/B of peak VmHWM, 1M single-member sets, pre-union vs union f3srv.
# Run on the GamingPC under WSL2. Build both binaries first:
#   git checkout 7a8e866 && go build -o /root/bin/f3srv-preunion ./cmd/f3srv
#   git checkout 7cf13d3 && go build -o /root/bin/f3srv          ./cmd/f3srv
set -u
PORT=7411
one() {
  local bin="$1"
  pkill -f "f3srv" 2>/dev/null; sleep 1
  GOMAXPROCS=14 taskset -c 4-17 "$bin" -addr 127.0.0.1:$PORT -shards 8 \
    -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 \
    -net goroutine >/tmp/rep.log 2>&1 &
  local pid=$!
  for i in $(seq 1 40); do /root/bin/redis-cli -p $PORT PING >/dev/null 2>&1 && break; sleep 0.25; done
  /root/bin/redis-cli -p $PORT FLUSHALL >/dev/null 2>&1
  taskset -c 18-31 /root/bin/redis-benchmark -p $PORT -r 1000000 -n 5000000 \
    -c 50 -q SADD set:__rand_int__ hello >/dev/null 2>&1
  sleep 1
  awk '/VmHWM/{print $2}' /proc/$pid/status 2>/dev/null
  kill $pid 2>/dev/null; sleep 1
}
for label in preunion union; do
  bin=/root/bin/f3srv; [ "$label" = preunion ] && bin=/root/bin/f3srv-preunion
  vals=""; for r in 1 2 3; do vals="$vals $(one "$bin")"; done
  echo "$label VmHWM_kB reps:$vals"
done
