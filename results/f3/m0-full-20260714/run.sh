#!/bin/bash
# Full f3 M0 gate matrix on GamingPC. Resumable through done.list.
set -euo pipefail

ROOT=${ROOT:-/root/f3gate/m0-full-20260714}
F3SRV=${F3SRV:-/root/f3gate/m0-gate-20260714/bin/f3srv}
AB=${AB:-/root/f3gate/ltm-gate/bin/aki-bench}
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=4-17
CMASK=18-31
PA=7511
PR=7512
PV=7513
WARM=3s
REPS=3

mkdir -p "$ROOT/cells" "$ROOT/tmp"
touch "$ROOT/done.list" "$ROOT/run.log"

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$ROOT/run.log"
}

wait_ping() {
  local port=$1
  for _ in $(seq 1 150); do
    "$CLI" -p "$port" ping >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

stop_servers() {
  local pid
  for pid in ${APID:-} ${RPID:-} ${VPID:-}; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in ${APID:-} ${RPID:-} ${VPID:-}; do
    wait "$pid" 2>/dev/null || true
  done
  APID= RPID= VPID=
}

trap stop_servers EXIT

start_servers() {
  local cell=$1 arena=$2 small=$3 ltm=${4:-false}
  local adir="$ROOT/tmp/$cell-aki" rdir="$ROOT/tmp/$cell-redis" vdir="$ROOT/tmp/$cell-valkey"
  mkdir -p "$adir" "$rdir" "$vdir"
  local aextra=() rextra=() vextra=()
  if [ "$small" = true ]; then
    aextra=(-batch-data-cap 1024 -reply-ring 128 -free-list-cap 8)
  fi
  if [ "$ltm" = true ]; then
    mkdir -p "$adir/vlog"
    aextra+=(-vlog-dir "$adir/vlog" -resident-cap-mib 64)
    rextra=(--maxmemory 512mb --maxmemory-policy allkeys-lfu)
    vextra=(--maxmemory 512mb --maxmemory-policy allkeys-lfu)
  fi

  GOMAXPROCS=14 taskset -c "$SMASK" "$F3SRV" -addr "127.0.0.1:$PA" \
    -shards 8 -arena-mib "$arena" "${aextra[@]}" \
    >"$ROOT/cells/$cell.aki-server.log" 2>&1 & APID=$!
  taskset -c "$SMASK" /root/bin/redis-server --port "$PR" --bind 127.0.0.1 \
    --save '' --appendonly no --io-threads 6 --dir "$rdir" "${rextra[@]}" \
    >"$ROOT/cells/$cell.redis-server.log" 2>&1 & RPID=$!
  taskset -c "$SMASK" /root/bin/valkey-server --port "$PV" --bind 127.0.0.1 \
    --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes \
    --dir "$vdir" "${vextra[@]}" \
    >"$ROOT/cells/$cell.valkey-server.log" 2>&1 & VPID=$!
  wait_ping "$PA"
  wait_ping "$PR"
  wait_ping "$PV"
  {
    echo "aki=$(taskset -cp "$APID" 2>&1)"
    echo "redis=$(taskset -cp "$RPID" 2>&1)"
    echo "valkey=$(taskset -cp "$VPID" 2>&1)"
  } >"$ROOT/cells/$cell.affinity.txt"
}

flush_all() {
  "$CLI" -p "$PA" flushall >/dev/null
  "$CLI" -p "$PR" flushall >/dev/null
  "$CLI" -p "$PV" flushall >/dev/null
}

snapshot() {
  local cell=$1 tag=$2
  {
    for item in "aki:$APID:$PA" "redis:$RPID:$PR" "valkey:$VPID:$PV"; do
      IFS=: read -r name pid port <<<"$item"
      echo "[$name]"
      awk '/VmRSS|VmHWM|VmSwap/{print}' "/proc/$pid/status"
      echo "dbsize=$($CLI -p "$port" dbsize 2>/dev/null || true)"
      "$CLI" -p "$port" info memory 2>/dev/null | sed -n '/^used_memory:/p'
    done
  } >"$ROOT/cells/$cell.$tag.memory.txt"
}

run_ab_cell() {
  local cell=$1 wl=$2 value=$3 keys=$4 dist=$5 conns=$6 arena=$7 duration=${8:-8s}
  local small=false
  [ "$value" = 16 ] || [ "$value" = 64 ] && small=true
  start_servers "$cell" "$arena" "$small"
  local rep rc
  for rep in $(seq 1 "$REPS"); do
    flush_all
    rc=0
    taskset -c "$CMASK" "$AB" -aki-addr "127.0.0.1:$PA" \
      -redis-addr "127.0.0.1:$PR" -valkey-addr "127.0.0.1:$PV" \
      -cpu-split=false -workload "$wl" -value-size "$value" -keys "$keys" \
      -dist "$dist" -connections "$conns" -pipeline 16 -warm "$WARM" \
      -duration "$duration" -json "$ROOT/cells/$cell.rep$rep.json" \
      >"$ROOT/cells/$cell.rep$rep.out" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
      echo "unexpected_exit=$rc" >>"$ROOT/cells/$cell.meta"
    fi
    snapshot "$cell" "rep$rep"
  done
  stop_servers
}

run_p1() {
  local conns cell="p1_set_64b_c$1" rep rc
  conns=$1
  start_servers "$cell" 512 true
  for rep in $(seq 1 "$REPS"); do
    flush_all
    rc=0
    taskset -c "$CMASK" "$AB" -aki-addr "127.0.0.1:$PA" \
      -redis-addr "127.0.0.1:$PR" -valkey-addr "127.0.0.1:$PV" \
      -cpu-split=false -workload set -value-size 64 -keys 1000000 \
      -dist uniform -connections "$conns" -pipeline 1 -warm "$WARM" \
      -duration 8s -json "$ROOT/cells/$cell.rep$rep.json" \
      >"$ROOT/cells/$cell.rep$rep.out" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
      echo "unexpected_exit=$rc" >>"$ROOT/cells/$cell.meta"
    fi
  done
  stop_servers
}

port_for() {
  case "$1" in aki) echo "$PA";; redis) echo "$PR";; valkey) echo "$PV";; esac
}

dual_rb() {
  local cell=$1 target=$2 rep=$3 count=$4
  shift 4
  local port prefix
  port=$(port_for "$target")
  prefix="$ROOT/cells/$cell.$target.rep$rep"
  taskset -c 18-24 "$RB" -p "$port" -n "$count" -r "$RB_KEYS" \
    -c 256 -P 16 --threads 7 --csv "$@" >"$prefix-a.csv" 2>"$prefix-a.err" &
  local p1=$!
  taskset -c 25-31 "$RB" -p "$port" -n "$count" -r "$RB_KEYS" \
    -c 256 -P 16 --threads 7 --csv "$@" >"$prefix-b.csv" 2>"$prefix-b.err" &
  local p2=$!
  wait "$p1"
  wait "$p2"
}

preload_rb() {
  local target=$1 value=$2 keys=$3
  local port n
  port=$(port_for "$target")
  n=$((keys * 5 / 2))
  taskset -c 18-24 "$RB" -p "$port" -t set -d "$value" -r "$keys" \
    -n "$n" -c 256 -P 16 --threads 7 -q >/dev/null 2>&1 &
  local p1=$!
  taskset -c 25-31 "$RB" -p "$port" -t set -d "$value" -r "$keys" \
    -n "$n" -c 256 -P 16 --threads 7 -q >/dev/null 2>&1 &
  local p2=$!
  wait "$p1"
  wait "$p2"
}

run_rb_cell() {
  local cell=$1 value=$2 keys=$3 arena=$4 count=$5
  shift 5
  local small=false target rep port
  [ "$value" -le 64 ] && small=true
  start_servers "$cell" "$arena" "$small"
  RB_KEYS=$keys
  for target in aki redis valkey; do
    port=$(port_for "$target")
    for rep in warm 1 2 3; do
      "$CLI" -p "$port" flushall >/dev/null
      preload_rb "$target" "$value" "$keys"
      dual_rb "$cell" "$target" "$rep" "$count" "$@"
    done
  done
  snapshot "$cell" final
  stop_servers
}

run_ltm_cell() {
  local cell=$1 wl=$2 rep rc
  start_servers "$cell" 256 false true
  for rep in $(seq 1 "$REPS"); do
    flush_all
    rc=0
    taskset -c "$CMASK" "$AB" -aki-addr "127.0.0.1:$PA" \
      -redis-addr "127.0.0.1:$PR" -valkey-addr "127.0.0.1:$PV" \
      -cpu-split=false -workload "$wl" -value-size 1032 -keys 1000000 \
      -dist uniform -connections 512 -pipeline 16 -warm "$WARM" \
      -duration 15s -coverage-probe 10000 \
      -json "$ROOT/cells/$cell.rep$rep.json" \
      >"$ROOT/cells/$cell.rep$rep.out" 2>&1 || rc=$?
    if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
      echo "unexpected_exit=$rc" >>"$ROOT/cells/$cell.meta"
    fi
    snapshot "$cell" "rep$rep"
  done
  stop_servers
}

run_all32() {
  local cell=$1 wl=$2
  local SMASK=0-15
  local CMASK=16-31
  run_ab_cell "$cell" "$wl" 64 1000000 uniform 512 512
}

run_cell() {
  local cell=$1
  shift
  if grep -qxF "$cell" "$ROOT/done.list"; then
    log "skip $cell"
    return
  fi
  log "start $cell"
  "$@"
  echo "$cell" >>"$ROOT/done.list"
  sync
  log "end $cell"
}

{
  date -u +'%Y-%m-%dT%H:%M:%SZ'
  uname -a
  nproc
  free -h
  go version
  echo 'aki_source=69aa50aeb688e2e2c17690f845a4ce342ea35a1c'
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  "$RB" --version
  sha256sum "$F3SRV" "$AB" /root/bin/redis-server /root/bin/valkey-server "$RB"
} >"$ROOT/env.txt"

# Point operations across value sizes.
run_cell set_16b_1m run_ab_cell set_16b_1m set 16 1000000 uniform 512 512
run_cell set_64b_1m run_ab_cell set_64b_1m set 64 1000000 uniform 512 512
run_cell set_256b_1m run_ab_cell set_256b_1m set 256 1000000 uniform 512 512
run_cell set_1k_1m run_ab_cell set_1k_1m set 1k 1000000 uniform 512 1024
run_cell set_4k_1m run_ab_cell set_4k_1m set 4k 1000000 uniform 512 2048
run_cell set_64k_100k run_ab_cell set_64k_100k set 64k 100000 uniform 512 3072
run_cell set_1m_4k run_ab_cell set_1m_4k set 1m 4000 uniform 64 3072
run_cell get_16b_1m run_ab_cell get_16b_1m get 16 1000000 uniform 512 512
run_cell get_64b_1m run_ab_cell get_64b_1m get 64 1000000 uniform 512 512
run_cell get_256b_1m run_ab_cell get_256b_1m get 256 1000000 uniform 512 512
run_cell get_1k_1m run_ab_cell get_1k_1m get 1k 1000000 uniform 512 1024
run_cell get_4k_1m run_ab_cell get_4k_1m get 4k 1000000 uniform 512 2048
run_cell get_64k_100k run_ab_cell get_64k_100k get 64k 100000 uniform 512 3072
run_cell get_1m_4k run_ab_cell get_1m_4k get 1m 4000 uniform 64 3072

# Cardinality, integer, distribution, and multi-key profiles.
run_cell set_64b_1k run_ab_cell set_64b_1k set 64 1000 uniform 512 512
run_cell set_64b_100k run_ab_cell set_64b_100k set 64 100000 uniform 512 512
run_cell get_64b_1k run_ab_cell get_64b_1k get 64 1000 uniform 512 512
run_cell get_64b_100k run_ab_cell get_64b_100k get 64 100000 uniform 512 512
run_cell incr_1k run_ab_cell incr_1k incr 64 1000 uniform 512 512
run_cell incr_100k run_ab_cell incr_100k incr 64 100000 uniform 512 512
run_cell incr_1m run_ab_cell incr_1m incr 64 1000000 uniform 512 512
run_cell set_64b_1m_zipf run_ab_cell set_64b_1m_zipf set 64 1000000 zipfian 512 512
run_cell get_64b_1m_zipf run_ab_cell get_64b_1m_zipf get 64 1000000 zipfian 512 512
run_cell set_1k_1m_zipf run_ab_cell set_1k_1m_zipf set 1k 1000000 zipfian 512 1024
run_cell get_1k_1m_zipf run_ab_cell get_1k_1m_zipf get 1k 1000000 zipfian 512 1024
run_cell mset_64b_1m run_ab_cell mset_64b_1m mset 64 1000000 uniform 512 512

# Native and redis-benchmark-only range/multi-key/update profiles.
run_cell getrange_1k_1m run_ab_cell getrange_1k_1m getrange 1k 1000000 uniform 512 1024
run_cell getrange_64k_100k run_ab_cell getrange_64k_100k getrange 64k 100000 uniform 512 3072
run_cell append_grow_1m run_ab_cell append_grow_1m append 16 1000000 uniform 512 1024
run_cell mget_64b_1m run_rb_cell mget_64b_1m 64 1000000 512 2000000 \
  mget key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ \
  key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ \
  key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ \
  key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__
run_cell getrange_rb_1k run_rb_cell getrange_rb_1k 1024 1000000 1024 3000000 \
  getrange key:__rand_int__ 500 599
run_cell getrange_rb_64k run_rb_cell getrange_rb_64k 65536 100000 3072 500000 \
  getrange key:__rand_int__ 32000 32099
run_cell setrange_rb_1k run_rb_cell setrange_rb_1k 1024 1000000 1024 3000000 \
  setrange key:__rand_int__ 500 \
  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
run_cell setrange_rb_64k run_rb_cell setrange_rb_64k 65536 100000 3072 500000 \
  setrange key:__rand_int__ 32000 \
  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
run_cell append_rb_1k run_rb_cell append_rb_1k 1024 1000000 2048 3000000 \
  append key:__rand_int__ xxxxxxxxxxxxxxxx
run_cell append_rb_64k run_rb_cell append_rb_64k 65536 100000 3072 500000 \
  append key:__rand_int__ xxxxxxxxxxxxxxxx

# Hot-key and recorded-only P1 profiles.
run_cell hot_set run_ab_cell hot_set set 64 1 uniform 512 512
run_cell hot_incr run_ab_cell hot_incr incr 64 1 uniform 512 512
run_cell p1_set_64b_c50 run_p1 50
run_cell p1_set_64b_c512 run_p1 512

# The M0 larger-than-memory pair, with coverage accounting enabled.
run_cell ltm_get run_ltm_cell ltm_get get
run_cell ltm_set run_ltm_cell ltm_set set

# Non-gating appendix profiles carried by the original M0 description.
run_cell sweep_set_256b_zipf run_ab_cell sweep_set_256b_zipf set 256 1000000 zipfian 512 512
run_cell sweep_get_256b_zipf run_ab_cell sweep_get_256b_zipf get 256 1000000 zipfian 512 512
run_cell sweep_set_4k_zipf run_ab_cell sweep_set_4k_zipf set 4k 1000000 zipfian 512 2048
run_cell sweep_get_4k_zipf run_ab_cell sweep_get_4k_zipf get 4k 1000000 zipfian 512 2048
run_cell sweep_set_64b_c50 run_ab_cell sweep_set_64b_c50 set 64 1000000 uniform 50 512
run_cell sweep_get_64b_c50 run_ab_cell sweep_get_64b_c50 get 64 1000000 uniform 50 512
run_cell sweep_mixed_64b run_ab_cell sweep_mixed_64b mixed 64 1000000 uniform 512 512
run_cell all32_set_64b run_all32 all32_set_64b set
run_cell all32_get_64b run_all32 all32_get_64b get

log "complete $(wc -l <"$ROOT/done.list") profiles"
touch "$ROOT/complete"
