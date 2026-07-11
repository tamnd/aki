#!/bin/bash
# f3 larger-than-memory fairness run (issue tamnd/aki#542).
# This replaces the single LTM posture from m0-run3, which pitted a
# full-dataset aki against rivals capped at maxmemory that had evicted half
# the keyspace and answered nil from RAM. See results/f3/ltm-protocol.md for
# the diagnosis and the accounting this run adds.
#
# Every LTM cell now runs two arms:
#   equal-cap:  all three servers get the same 512MiB memory ceiling. The
#               rivals evict under it; aki spills to the vlog. The honest
#               comparison is data-bearing throughput AND dataset coverage
#               AND peak memory, not raw ops, because a rival's ops here
#               include the nils it returns for keys it dropped.
#   equal-data: the rivals get a maxmemory large enough to hold the ENTIRE
#               dataset with noeviction, so they keep 100% coverage; aki keeps
#               the same small 512MiB resident cap and serves the rest off the
#               vlog. This is the product pitch: same coverage, far less
#               resident memory. Compare data-bearing throughput at equal
#               coverage, and compare peak memory and bytes per retrievable key.
#
# aki-bench is the sole load harness on every LTM cell: redis-benchmark counts
# a rival's nil replies as completed ops, which is the exact bug this run
# exists to expose, so it stays banned here. aki-bench's -coverage-probe
# samples the full keyspace after each window and reports the retrievable
# fraction per server; this script reads VmRSS and VmHWM from /proc for all
# three servers, since aki-bench in connect mode cannot read a rival's PID.
#
# Protocol: -warm 3s, 3 timed windows of 20s, FLUSHALL between reps (aki-bench
# re-preloads), server cpus 0-7, generator 8-15, rivals io-threads 4.
# Resumable via $G/done.list. Measurement only; no repo code changes.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/ltm-gate
F3SRV=$G/bin/f3srv
AB=$G/bin/aki-bench
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
PA=7111; PR=7112; PV=7113
RT=4
VT=4
WARM=3s
DUR=20s

# Dataset: 2M keys x 1032B is about 2GB of values, four times the 512MiB cap,
# so the equal-cap rivals must evict roughly three quarters of it and the
# equal-data rivals must hold all of it in RAM.
NKEYS=2000000
VBYTES=1032
CONNS=64
PIPE=16
SHARDS=4
ARENA=256
RESCAP=128     # per shard; 4 x 128 = 512MiB total resident cap
CAP_MB=512     # equal-cap rival maxmemory
DATA_MB=4096   # equal-data rival maxmemory, comfortably above the 2GB dataset
COVSAMPLE=100000

APID=""; RPID=""; VPID=""
DIR_A=""; DIR_R=""; DIR_V=""
CELL=""
SAMPLER=""

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
meta() { echo "$*" >> "$G/cells/$CELL.meta"; }

wait_ping() {
  for i in $(seq 1 150); do
    $CLI -p "$1" ping >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  return 1
}

port_closed_wait() {
  for i in $(seq 1 100); do
    $CLI -p "$1" -t 1 ping >/dev/null 2>&1 || return 0
    sleep 0.2
  done
  return 1
}

flush_all() {
  local p
  for p in $PA $PR $PV; do $CLI -p $p flushall >/dev/null 2>&1; done
  sleep 0.5
}

# reset_peak zeroes each server's VmHWM to its current RSS so the peak we read
# after the load is the peak reached DURING this cell, not a stale high-water
# mark left over from a previous phase. clear_refs value 5 resets the peak.
reset_peak() {
  local p
  for p in $APID $RPID $VPID; do
    echo 5 > /proc/$p/clear_refs 2>/dev/null
  done
}

start_sampler() { # 1s VmRSS + VmHWM sampler for all three servers, kB
  (
    while :; do
      echo "$(date -u +%s) \
aki_rss=$(awk '/VmRSS/{print $2}' /proc/$APID/status 2>/dev/null) aki_hwm=$(awk '/VmHWM/{print $2}' /proc/$APID/status 2>/dev/null) \
redis_rss=$(awk '/VmRSS/{print $2}' /proc/$RPID/status 2>/dev/null) redis_hwm=$(awk '/VmHWM/{print $2}' /proc/$RPID/status 2>/dev/null) \
valkey_rss=$(awk '/VmRSS/{print $2}' /proc/$VPID/status 2>/dev/null) valkey_hwm=$(awk '/VmHWM/{print $2}' /proc/$VPID/status 2>/dev/null)"
      sleep 1
    done
  ) >> "$G/cells/$CELL.rss.samples" 2>/dev/null &
  SAMPLER=$!
}

stop_sampler() {
  [ -n "$SAMPLER" ] && kill $SAMPLER 2>/dev/null
  SAMPLER=""
}

# mem_snap records VmRSS and VmHWM (peak) for all three servers, plus each
# server's own used_memory and dbsize. VmHWM is the peak column the fairness
# protocol adds; it travels next to steady RSS because the LTM pitch is a
# memory-ceiling claim and a peak above a rival breaks it even at a lower
# settled RSS.
mem_snap() { # tag
  local tag=$1 out="$G/cells/$CELL.meta"
  {
    echo "rss[$tag] aki=$(awk '/VmRSS/{print $2$3}' /proc/$APID/status 2>/dev/null) redis=$(awk '/VmRSS/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmRSS/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
    echo "hwm[$tag] aki=$(awk '/VmHWM/{print $2$3}' /proc/$APID/status 2>/dev/null) redis=$(awk '/VmHWM/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmHWM/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
    echo "used_memory[$tag] aki=$($CLI -p $PA info 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r') redis=$($CLI -p $PR info memory 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r') valkey=$($CLI -p $PV info memory 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r')"
    echo "dbsize[$tag] aki=$($CLI -p $PA dbsize 2>/dev/null) redis=$($CLI -p $PR dbsize 2>/dev/null) valkey=$($CLI -p $PV dbsize 2>/dev/null)"
  } >> "$out"
}

rival_info_snap() { # tag: rivals' hit/miss/evicted counters
  local tag=$1 p name
  for name in redis:$PR valkey:$PV; do
    p=${name#*:}
    echo "rival_info[$tag][${name%%:*}] $($CLI -p $p info stats 2>/dev/null | grep -E '^(keyspace_hits|keyspace_misses|evicted_keys):' | tr -d '\r' | tr '\n' ' ')" >> "$G/cells/$CELL.meta"
  done
}

check_alive() {
  if ! kill -0 $APID 2>/dev/null || ! $CLI -p $PA ping >/dev/null 2>&1; then
    meta "CRASH f3srv dead after $1"
    cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log 2>/dev/null
    return 1
  fi
  return 0
}

# ab_run drives one aki-bench window across all three connect-mode targets and
# runs the post-window coverage probe. -coverage-probe samples the full
# keyspace and records, per target, the fraction of keys that come back with
# the exact written length and content; a rival that evicted keys reports them
# here as misses.
ab_run() { # rep flags...
  local rep=$1; shift
  local out="$G/cells/$CELL.ab.rep$rep"
  taskset -c $CMASK $AB -aki-addr 127.0.0.1:$PA -redis-addr 127.0.0.1:$PR -valkey-addr 127.0.0.1:$PV \
    -cpu-server $SMASK -cpu-client $CMASK \
    -warm $WARM -coverage-probe $COVSAMPLE "$@" -json "$out.json" > "$out.out" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && [ $rc -ne 2 ] && meta "ab rep$rep exit=$rc"
  if grep -qi "dataset coverage" "$out.out" 2>/dev/null; then
    meta "COVERAGE rep$rep: $(grep -i 'dataset coverage' $out.out | tr '\n' '; ')"
  fi
  check_alive "ab rep$rep" || return 1
  return 0
}

start_servers() { # $1 rival maxmemory (MB)  $2 rival policy
  local capmb=$1 policy=$2
  DIR_A=$(mktemp -d $G/tmp/a.XXXXXX)
  local vlog=$DIR_A/vlog; mkdir -p "$vlog"
  DIR_R=$(mktemp -d $G/tmp/r.XXXXXX)
  DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  # aki keeps the same small resident cap on both arms; only the rivals' ceiling
  # changes between equal-cap and equal-data. That is the whole design: hold
  # aki's memory fixed and small, and vary what the rivals are allowed.
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PA --shards $SHARDS \
    --arena-mib $ARENA --resident-cap-mib $RESCAP --vlog-dir "$vlog" >$DIR_A/f3srv.log 2>&1 &
  APID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $RT --dir $DIR_R \
    --maxmemory ${capmb}mb --maxmemory-policy $policy >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $VT --io-threads-do-reads yes --dir $DIR_V \
    --maxmemory ${capmb}mb --maxmemory-policy $policy >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  wait_ping $PA || { meta "FATAL f3srv did not answer PING"; cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log; return 1; }
  wait_ping $PR || { meta "FATAL redis did not answer PING"; return 1; }
  wait_ping $PV || { meta "FATAL valkey did not answer PING"; return 1; }
  meta "aki posture: shards=$SHARDS arena_mib=$ARENA resident_cap_mib=$RESCAP (total cap ${CAP_MB}MiB) vlog=$vlog"
  meta "rival posture: --maxmemory ${capmb}mb --maxmemory-policy $policy io-threads=4"
  meta "vlog_fs $(stat -f -c %T $vlog 2>/dev/null || stat -f %T $vlog 2>/dev/null)"
  meta "shards_banner $(grep -o 'with [0-9]* shards' $DIR_A/f3srv.log | head -1)"
  return 0
}

stop_servers() {
  stop_sampler
  meta "vlog_du $(du -sh $DIR_A/vlog 2>/dev/null)"
  for p in $APID $RPID $VPID; do kill $p 2>/dev/null; done
  for p in $APID $RPID $VPID; do wait $p 2>/dev/null; done
  port_closed_wait $PA; port_closed_wait $PR; port_closed_wait $PV
  if [ -f "$DIR_A/f3srv.log" ] && [ "$(wc -l < $DIR_A/f3srv.log)" -gt 1 ]; then
    cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log
  fi
  rm -rf "$DIR_A" "$DIR_R" "$DIR_V"
  APID=""; RPID=""; VPID=""
}

# ltm_arm runs one arm of an LTM cell.
#   $1 workload (get|set)  $2 dist (uniform|zipfian)
#   $3 rival maxmemory MB  $4 rival policy
ltm_arm() {
  local wl=$1 dist=$2 capmb=$3 policy=$4
  start_servers "$capmb" "$policy" || { stop_servers; return; }
  meta "cell LTM wl=$wl dist=$dist keys=$NKEYS value=${VBYTES}B conns=$CONNS pipeline=$PIPE warm=$WARM dur=$DUR cov_sample=$COVSAMPLE"
  local abf="-workload $wl -value-size $VBYTES -keys $NKEYS -dist $dist -connections $CONNS -pipeline $PIPE -duration $DUR"
  [ "$dist" = zipfian ] && abf="$abf -zipf-s 0.99"
  mem_snap launch
  start_sampler
  rival_info_snap pre-rep0
  local r
  for r in 0 1 2; do
    flush_all
    reset_peak
    ab_run $r $abf || { stop_servers; return; }
    mem_snap post-rep$r
    rival_info_snap post-rep$r
  done
  stop_servers
}

run_cell() {
  CELL=$1; shift
  if grep -qxF "$CELL" $G/done.list 2>/dev/null; then log "skip $CELL (done)"; return; fi
  rm -f "$G/cells/$CELL."* 2>/dev/null
  log "cell $CELL start"
  "$@"
  echo "$CELL" >> $G/done.list
  sync
  log "cell $CELL end"
  sleep 5
}

mkdir -p $G/cells $G/tmp
touch $G/done.list

{
  date
  uname -a
  echo "aki commit: $(cd /root/aki && git rev-parse HEAD)"
  echo "aki-bench commit: $(cd /root/f3gate/aki-bench && git rev-parse HEAD)"
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  go version
  free -h
  uptime
} > $G/env.txt 2>&1

log "ltm-gate starting; aki $(cd /root/aki && git rev-parse --short HEAD), aki-bench $(cd /root/f3gate/aki-bench && git rev-parse --short HEAD)"
pkill -f "f3srv --addr 127.0.0.1:71" 2>/dev/null; sleep 1

# Equal-cap arm: all three at 512MiB, rivals evict under allkeys-lfu.
run_cell ecap_get_uniform ltm_arm get uniform $CAP_MB  allkeys-lfu
run_cell ecap_get_zipf    ltm_arm get zipfian $CAP_MB  allkeys-lfu
run_cell ecap_set_uniform ltm_arm set uniform $CAP_MB  allkeys-lfu

# Equal-data arm: rivals get 4GB and noeviction so they keep the whole dataset;
# aki keeps its 512MiB resident cap. The product-pitch comparison.
run_cell edata_get_uniform ltm_arm get uniform $DATA_MB noeviction
run_cell edata_get_zipf    ltm_arm get zipfian $DATA_MB noeviction
run_cell edata_set_uniform ltm_arm set uniform $DATA_MB noeviction

log "ltm-gate complete: $(wc -l < $G/done.list) cells done"
