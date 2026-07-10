#!/bin/bash
# Lab 19 driver: reactor loop-count sweep on the gate box (see README.md).
# Server taskset $SMASK, generator $CMASK, redis-benchmark --threads 4,
# 3 reps of n=20M per arm, GET preloaded, VmRSS snapshot after the last rep.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=${G:-/root/f3gate/reactor-ab/lab19}
F3SRV=${F3SRV:-/root/f3gate/reactor-ab/bin/f3srv}
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
PORT=7111
KEYS=1000000
N=20000000

mkdir -p "$G/cells"
touch "$G/done.list"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

wait_ping() {
  for i in $(seq 1 150); do
    $CLI -p $PORT ping >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  return 1
}

port_closed_wait() {
  for i in $(seq 1 100); do
    $CLI -p $PORT -t 1 ping >/dev/null 2>&1 || return 0
    sleep 0.2
  done
  return 1
}

# arm <cellname> <shards-args> <loops> <wl>
arm() {
  local cell=$1 sargs=$2 loops=$3 wl=$4
  grep -qxF "$cell" "$G/done.list" 2>/dev/null && { log "skip $cell"; return; }
  rm -f "$G/cells/$cell."*
  log "arm $cell start"
  local dir; dir=$(mktemp -d "$G/tmp.XXXXXX")
  # shellcheck disable=SC2086
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PORT $sargs --arena-mib 512 \
    -net reactor -net-loops "$loops" >"$dir/f3srv.log" 2>&1 &
  local pid=$!
  wait_ping || { echo "FATAL no ping" >"$G/cells/$cell.meta"; cp "$dir/f3srv.log" "$G/cells/$cell.f3srv.log"; kill $pid; rm -rf "$dir"; return 1; }
  echo "pins $(taskset -cp $pid 2>&1)" >"$G/cells/$cell.meta"
  echo "banner $(grep -o 'with [0-9]* shards' "$dir/f3srv.log" | head -1) loops=$loops" >>"$G/cells/$cell.meta"
  if [ "$wl" = get ]; then
    taskset -c $CMASK $RB -p $PORT -t set -d 64 -r $KEYS -n 4000000 -c 64 -P 64 --threads 4 -q >/dev/null 2>&1
  fi
  local r
  for r in 0 1 2; do
    [ "$wl" = set ] && $CLI -p $PORT flushall >/dev/null 2>&1 && sleep 0.5
    taskset -c $CMASK $RB -p $PORT -t "$wl" -d 64 -r $KEYS -n $N -c 512 -P 16 --threads 4 --csv \
      >"$G/cells/$cell.rb.rep$r.csv" 2>"$G/cells/$cell.rb.rep$r.err"
    if ! kill -0 $pid 2>/dev/null || ! $CLI -p $PORT ping >/dev/null 2>&1; then
      echo "CRASH after rep$r" >>"$G/cells/$cell.meta"
      cp "$dir/f3srv.log" "$G/cells/$cell.f3srv.log"
      kill $pid 2>/dev/null; wait $pid 2>/dev/null; rm -rf "$dir"; return 1
    fi
  done
  echo "rss_post VmRSS=$(awk '/VmRSS/{print $2$3}' /proc/$pid/status)" >>"$G/cells/$cell.meta"
  echo "dbsize $($CLI -p $PORT dbsize)" >>"$G/cells/$cell.meta"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null
  port_closed_wait
  if [ "$(wc -l <"$dir/f3srv.log")" -gt 1 ]; then cp "$dir/f3srv.log" "$G/cells/$cell.f3srv.log"; fi
  rm -rf "$dir"
  echo "$cell" >>"$G/done.list"
  log "arm $cell end"
  sleep 3
}

{
  date; uname -a
  $RB --version
  echo "f3srv sha256: $(sha256sum $F3SRV)"
} >"$G/env.txt" 2>&1

pkill -f "f3srv --addr 127.0.0.1:$PORT" 2>/dev/null; sleep 1

# Main sweep at default shards (4 on the 8-cpu server mask).
for L in 1 2 3 4 6 8; do
  arm "get_loops$L" "" "$L" get
done
for L in 1 2 3 4 6 8; do
  arm "set_loops$L" "" "$L" set
done

# Interleaved confirmation pass for the top contenders (session-drift check).
for L in 3 4 6; do
  arm "get_loops${L}_pass2" "" "$L" get
done

# Disambiguation arm: shards=5 splits the two formulas (cores-shards=3 vs shards=5).
for L in 3 5; do
  arm "s5_get_loops$L" "--shards 5" "$L" get
done

log "lab19 sweep complete"
