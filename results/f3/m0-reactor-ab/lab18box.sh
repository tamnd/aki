#!/bin/bash
# Lab 18 owed box numbers: (a) the in-process counter sweep from the lab's
# README, same shape as the container run (4 shards, 4 loops, conns x depth),
# on the gate box; (b) the slice 4 throughput A/B the README owes, GET 64B
# P16/512 on the reactor, main (wake-batched) vs d81a66b (pre-batch old arm),
# interleaved 3 reps per arm, both at -net-loops 3 so the loop count is not a
# variable.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/reactor-ab/lab18
NEW=/root/f3gate/reactor-ab/bin/f3srv
OLD=/root/f3gate/reactor-ab/bin/f3srv-d81a66b
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
PORT=7111
mkdir -p "$G"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# (a) counter sweep, in process, client and server share cpus 0-15 like the
# container run shared its 5 vcpus.
if [ ! -f "$G/sweep.txt" ]; then
  log "counter sweep start"
  cd /root/aki
  taskset -c 0-15 go run ./labs/f3/m0/18_wake_batch -dur 4s > "$G/sweep.txt" 2>&1
  log "counter sweep done"
fi

# (b) old-arm build.
if [ ! -x "$OLD" ]; then
  cd /root/aki
  git worktree remove -f /root/aki-old 2>/dev/null
  git worktree add -f /root/aki-old d81a66b
  cd /root/aki-old && go build -o "$OLD" ./cmd/f3srv
  cd /root/aki && git worktree remove -f /root/aki-old
fi
sha256sum "$NEW" "$OLD" > "$G/binaries.sha256"

wait_ping() {
  for i in $(seq 1 150); do
    $CLI -p $PORT ping >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  return 1
}

one_arm() { # arm bin rep
  local arm=$1 bin=$2 r=$3
  taskset -c $SMASK "$bin" --addr 127.0.0.1:$PORT --arena-mib 512 \
    -net reactor -net-loops 3 >"$G/$arm.rep$r.f3srv.log" 2>&1 &
  local pid=$!
  wait_ping || { echo "FATAL no ping $arm rep$r" >>"$G/ab.meta"; kill $pid; return 1; }
  taskset -c $CMASK $RB -p $PORT -t set -d 64 -r 1000000 -n 4000000 -c 64 -P 64 --threads 4 -q >/dev/null 2>&1
  taskset -c $CMASK $RB -p $PORT -t get -d 64 -r 1000000 -n 20000000 -c 512 -P 16 --threads 4 --csv \
    >"$G/$arm.rep$r.rb.csv" 2>"$G/$arm.rep$r.rb.err"
  echo "rss[$arm rep$r] $(awk '/VmRSS/{print $2$3}' /proc/$pid/status)" >>"$G/ab.meta"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null
  for i in $(seq 1 100); do $CLI -p $PORT -t 1 ping >/dev/null 2>&1 || break; sleep 0.2; done
  sleep 2
}

if [ ! -f "$G/ab.done" ]; then
  log "wake-batch A/B start"
  : >"$G/ab.meta"
  for r in 0 1 2; do
    one_arm old "$OLD" $r
    one_arm new "$NEW" $r
  done
  touch "$G/ab.done"
  log "wake-batch A/B done"
fi
log "lab18 box complete"
