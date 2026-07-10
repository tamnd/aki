#!/bin/bash
# f3 M0 headline re-run. Fixed harness protocol (doc 18, this week's fixes):
#  - aki-bench with -warm 3s per timed window, 3 timed windows, discard none
#  - FLUSHALL on ALL servers between reps so memory rows are fair
#  - rivals also measured with redis-benchmark --threads 4; rival = min(ab, rb)
#  - same cpu split as the gate run: server 0-7, client 8-15, io-threads 4
# Resumable via $G/done.list.
set -u
export PATH=/root/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/m0-rerun
F3SRV=$G/bin/f3srv
AB=$G/bin/aki-bench
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
PA=7111; PR=7112; PV=7113
RT=4
VT=4
WARM=3s

APID=""; RPID=""; VPID=""
DIR_A=""; DIR_R=""; DIR_V=""
CELL=""

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
meta() { echo "$*" >> "$G/cells/$CELL.meta"; }

wait_ping() {
  for i in $(seq 1 100); do
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

flush_all() { # FLUSHALL on all three servers; fairness between reps
  local p
  for p in $PA $PR $PV; do $CLI -p $p flushall >/dev/null 2>&1; done
  sleep 0.5
}

start_servers() { # $1 extra f3srv args
  local aextra="$1"
  DIR_A=$(mktemp -d $G/tmp/a.XXXXXX)
  DIR_R=$(mktemp -d $G/tmp/r.XXXXXX)
  DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PA $aextra >$DIR_A/f3srv.log 2>&1 &
  APID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $RT --dir $DIR_R >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $VT --io-threads-do-reads yes --dir $DIR_V >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  wait_ping $PA || { meta "FATAL f3srv did not answer PING"; cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log; return 1; }
  wait_ping $PR || { meta "FATAL redis did not answer PING"; return 1; }
  wait_ping $PV || { meta "FATAL valkey did not answer PING"; return 1; }
  meta "cf2_pins aki=$(taskset -cp $APID 2>&1) redis=$(taskset -cp $RPID 2>&1) valkey=$(taskset -cp $VPID 2>&1)"
  meta "launch aki_extra='$aextra' io_threads redis=$RT valkey=$VT warm=$WARM"
  meta "shards_banner $(grep -o 'with [0-9]* shards' $DIR_A/f3srv.log | head -1)"
  rss_snap launch
  return 0
}

stop_servers() {
  for p in $APID $RPID $VPID; do kill $p 2>/dev/null; done
  for p in $APID $RPID $VPID; do wait $p 2>/dev/null; done
  port_closed_wait $PA; port_closed_wait $PR; port_closed_wait $PV
  if [ -f "$DIR_A/f3srv.log" ] && [ "$(wc -l < $DIR_A/f3srv.log)" -gt 1 ]; then
    cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log
  fi
  rm -rf "$DIR_A" "$DIR_R" "$DIR_V"
  APID=""; RPID=""; VPID=""
}

check_alive() {
  if ! kill -0 $APID 2>/dev/null || ! $CLI -p $PA ping >/dev/null 2>&1; then
    meta "CRASH f3srv dead after $1"
    cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log 2>/dev/null
    return 1
  fi
  return 0
}

rss_snap() { # tag
  local tag=$1 out="$G/cells/$CELL.meta"
  {
    echo "rss[$tag] aki=$(awk '/VmRSS/{print $2$3}' /proc/$APID/status 2>/dev/null) redis=$(awk '/VmRSS/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmRSS/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
    echo "used_memory[$tag] aki=$($CLI -p $PA info 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r') redis=$($CLI -p $PR info memory 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r') valkey=$($CLI -p $PV info memory 2>/dev/null | awk -F: '/^used_memory:/{print $2}' | tr -d '\r')"
    echo "dbsize[$tag] aki=$($CLI -p $PA dbsize 2>/dev/null) redis=$($CLI -p $PR dbsize 2>/dev/null) valkey=$($CLI -p $PV dbsize 2>/dev/null)"
  } >> "$out"
}

info_snap() {
  echo "--- f3info[$1]" >> "$G/cells/$CELL.meta"
  $CLI -p $PA info 2>/dev/null >> "$G/cells/$CELL.meta"
}

ab_run() { # rep flags...
  local rep=$1; shift
  local out="$G/cells/$CELL.ab.rep$rep"
  taskset -c $CMASK $AB -aki-addr 127.0.0.1:$PA -redis-addr 127.0.0.1:$PR -valkey-addr 127.0.0.1:$PV \
    -cpu-server $SMASK -cpu-client $CMASK \
    -warm $WARM "$@" -json "$out.json" > "$out.out" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && [ $rc -ne 2 ] && meta "ab rep$rep exit=$rc"
  if grep -qi "generator.bound" "$out.out" "$out.json" 2>/dev/null; then
    meta "GENBOUND flagged in rep$rep"
  fi
  check_alive "ab rep$rep" || return 1
  return 0
}

rb_one() { # target rep n args...
  local tgt=$1 rep=$2 n=$3; shift 3
  local port
  case $tgt in aki) port=$PA;; redis) port=$PR;; valkey) port=$PV;; esac
  taskset -c $CMASK $RB -p $port -n "$n" --csv "$@" > "$G/cells/$CELL.rb.$tgt.rep$rep.csv" 2>"$G/cells/$CELL.rb.$tgt.rep$rep.err"
  check_alive "rb $tgt rep$rep" || return 1
  return 0
}

rb_preload() { # target n d keys
  local tgt=$1 n=$2 d=$3 keys=$4 port
  case $tgt in aki) port=$PA;; redis) port=$PR;; valkey) port=$PV;; esac
  taskset -c $CMASK $RB -p $port -t set -d "$d" -r "$keys" -n "$n" -c 64 -P 64 --threads 4 -q >/dev/null 2>&1
}

# std_cell: aki-bench 3 warmed timed windows with FLUSHALL between reps, then
# redis-benchmark --threads 4 x3 reps per target with FLUSHALL between targets.
#   $1 workload $2 value-tok $3 value-bytes $4 keys $5 dist $6 conns
#   $7 rb-kind (set|get|incr|hotset|none) $8 rb-n $9 arena-mib $10 ab-duration
std_cell() {
  local wl=$1 vtok=$2 vbytes=$3 keys=$4 dist=$5 conns=$6 rbkind=$7 rbn=$8 arena=$9 dur=${10}
  start_servers "--arena-mib $arena" || { stop_servers; return; }
  meta "cell wl=$wl value=$vtok keys=$keys dist=$dist conns=$conns pipeline=16 arena_mib_per_shard=$arena warm=$WARM dur=$dur"
  local abf="-workload $wl -value-size $vtok -keys $keys -dist $dist -connections $conns -pipeline 16 -duration $dur"
  local r
  for r in 0 1 2; do
    flush_all
    ab_run $r $abf || { stop_servers; return; }
    [ $r -eq 0 ] && rss_snap post-rep0
  done
  rss_snap post-ab
  if [ "$rbkind" != none ] && [ "$dist" = uniform ]; then
    local targs=() tgt
    case $rbkind in
      set)    targs=(-t set -d "$vbytes" -r "$keys" -c "$conns" -P 16 --threads 4);;
      get)    targs=(-t get -d "$vbytes" -r "$keys" -c "$conns" -P 16 --threads 4);;
      incr)   targs=(-t incr -r "$keys" -c "$conns" -P 16 --threads 4);;
      hotset) targs=(-t set -d "$vbytes" -c "$conns" -P 16 --threads 4);;
    esac
    for tgt in aki redis valkey; do
      flush_all
      if [ "$rbkind" = get ]; then
        local pn=$((keys*4)); [ $pn -gt 4000000 ] && pn=4000000
        rb_preload $tgt $pn "$vbytes" "$keys"
      fi
      for r in 0 1 2; do
        rb_one $tgt $r "$rbn" "${targs[@]}" || { stop_servers; return; }
      done
    done
  else
    meta "rb: n/a ($rbkind/$dist) aki-bench is the sole harness for this row"
  fi
  rss_snap post-rb
  info_snap end
  stop_servers
}

# sustained_cell: SET 4KiB sustained overwrite. NO flush inside a phase; three
# consecutive 20s warmed windows keep overwriting the same 1M keyspace so the
# arena has to reclaim (the shape that used to die on arena full). FLUSHALL
# only between the aki-bench phase and the redis-benchmark phase.
sustained_cell() {
  start_servers "--arena-mib 2048" || { stop_servers; return; }
  meta "cell SUSTAINED wl=set value=4k keys=1000000 dist=uniform conns=512 pipeline=16 arena_mib_per_shard=2048 warm=$WARM dur=20s no-flush-within-phase"
  local abf="-workload set -value-size 4k -keys 1000000 -dist uniform -connections 512 -pipeline 16 -duration 20s"
  local r
  for r in 0 1 2; do
    ab_run $r $abf || { stop_servers; return; }
    rss_snap "post-rep$r"
  done
  rss_snap post-ab
  info_snap post-ab
  local tgt
  for tgt in aki redis valkey; do
    flush_all
    for r in 0 1 2; do
      rb_one $tgt $r 4000000 -t set -d 4096 -r 1000000 -c 512 -P 16 --threads 4 || { stop_servers; return; }
    done
  done
  rss_snap post-rb
  info_snap end
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
log "m0 rerun starting; aki $(cd /root/aki && git rev-parse --short HEAD), aki-bench $(cd /root/f3gate/aki-bench && git rev-parse --short HEAD)"
pkill -f "f3srv --addr 127.0.0.1:71" 2>/dev/null; sleep 1

#                                wl   vtok vbytes keys    dist    conns rbkind rbn      arena dur
run_cell set_64b_1m    std_cell set  64   64   1000000 uniform 512  set    20000000 512  8s
run_cell get_64b_1m    std_cell get  64   64   1000000 uniform 512  get    20000000 512  8s
run_cell incr_1m       std_cell incr 64   64   1000000 uniform 512  incr   20000000 512  8s
run_cell set_1k_1m     std_cell set  1k   1024 1000000 uniform 512  set    10000000 1024 8s
run_cell get_1k_1m     std_cell get  1k   1024 1000000 uniform 512  get    10000000 1024 8s
run_cell get_64b_1m_zipf std_cell get 64  64   1000000 zipfian 512  none   0        512  8s
run_cell hot_set       std_cell set  64   64   1       uniform 512  hotset 20000000 512  8s
run_cell set_4k_sustained sustained_cell

log "m0 rerun complete: $(wc -l < $G/done.list) cells done"
