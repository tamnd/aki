#!/bin/bash
# f3 arena-rss A/B matrix (issue #542, labs/f3/m0/20_arena_rss).
# Four aki arms resident at once, old and new binaries in both driver shapes
# (osingle 7111, oreactor 7114, nsingle 7115, nreactor 7116), plus redis 7112
# and valkey 7113, all taskset 0-7; generator 8-15. Arms alternate within each
# rep (same-session interleaving) and every aki-bench invocation measures its
# arm AND both rivals, so per-arm ratios are same-invocation. Protocol per
# m0-run3: warm 3s, 3 timed 8s windows, FLUSHALL between reps on all servers,
# 1M keys uniform, redis-benchmark --threads 4 with -P matching the pipeline.
# Resumable via $G/done.list.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/rss-ab/matrix
F3OLD=/root/f3gate/rss-ab/bin/f3srv-old
F3NEW=/root/f3gate/rss-ab/bin/f3srv-new
AB=/root/f3gate/reactor-ab/bin/aki-bench
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
POS=7111; PR=7112; PV=7113; POR=7114; PNS=7115; PNR=7116
NL=${NL:-3}
WARM=3s
KEYS=1000000

OSPID=""; ORPID=""; NSPID=""; NRPID=""; RPID=""; VPID=""
DIR_OS=""; DIR_OR=""; DIR_NS=""; DIR_NR=""; DIR_R=""; DIR_V=""
CELL=""

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
meta() { echo "$*" >> "$G/cells/$CELL.meta"; }

port_of() { case $1 in osingle) echo $POS;; oreactor) echo $POR;; nsingle) echo $PNS;; nreactor) echo $PNR;; redis) echo $PR;; valkey) echo $PV;; esac; }
pid_of() { case $1 in osingle) echo $OSPID;; oreactor) echo $ORPID;; nsingle) echo $NSPID;; nreactor) echo $NRPID;; redis) echo $RPID;; valkey) echo $VPID;; esac; }

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
  for p in $POS $POR $PNS $PNR $PR $PV; do $CLI -p $p flushall >/dev/null 2>&1; done
  sleep 0.5
}

rss_snap() { # tag
  meta "rss[$1] osingle=$(awk '/VmRSS/{print $2$3}' /proc/$OSPID/status 2>/dev/null) oreactor=$(awk '/VmRSS/{print $2$3}' /proc/$ORPID/status 2>/dev/null) nsingle=$(awk '/VmRSS/{print $2$3}' /proc/$NSPID/status 2>/dev/null) nreactor=$(awk '/VmRSS/{print $2$3}' /proc/$NRPID/status 2>/dev/null) redis=$(awk '/VmRSS/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmRSS/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
  meta "hwm[$1] osingle=$(awk '/VmHWM/{print $2$3}' /proc/$OSPID/status 2>/dev/null) oreactor=$(awk '/VmHWM/{print $2$3}' /proc/$ORPID/status 2>/dev/null) nsingle=$(awk '/VmHWM/{print $2$3}' /proc/$NSPID/status 2>/dev/null) nreactor=$(awk '/VmHWM/{print $2$3}' /proc/$NRPID/status 2>/dev/null) redis=$(awk '/VmHWM/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmHWM/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
}

start_servers() { # $1 arena-mib
  local arena=$1
  DIR_OS=$(mktemp -d $G/tmp/os.XXXXXX); DIR_OR=$(mktemp -d $G/tmp/or.XXXXXX)
  DIR_NS=$(mktemp -d $G/tmp/ns.XXXXXX); DIR_NR=$(mktemp -d $G/tmp/nr.XXXXXX)
  DIR_R=$(mktemp -d $G/tmp/r.XXXXXX); DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  taskset -c $SMASK $F3OLD --addr 127.0.0.1:$POS --arena-mib $arena >$DIR_OS/f3srv.log 2>&1 &
  OSPID=$!
  taskset -c $SMASK $F3OLD --addr 127.0.0.1:$POR --arena-mib $arena -net reactor -net-loops $NL >$DIR_OR/f3srv.log 2>&1 &
  ORPID=$!
  taskset -c $SMASK $F3NEW --addr 127.0.0.1:$PNS --arena-mib $arena >$DIR_NS/f3srv.log 2>&1 &
  NSPID=$!
  taskset -c $SMASK $F3NEW --addr 127.0.0.1:$PNR --arena-mib $arena -net reactor -net-loops $NL >$DIR_NR/f3srv.log 2>&1 &
  NRPID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads 4 --dir $DIR_R >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads 4 --io-threads-do-reads yes --dir $DIR_V >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  local p
  for p in $POS $POR $PNS $PNR $PR $PV; do
    wait_ping $p || { meta "FATAL port $p did not answer PING"; return 1; }
  done
  meta "launch arena_mib=$arena net_loops=$NL warm=$WARM io_threads=4"
  meta "driver_stamp osingle=$($CLI -p $POS info 2>/dev/null | grep -o 'net_driver:[a-z]*') oreactor=$($CLI -p $POR info 2>/dev/null | grep -o 'net_driver:[a-z]*') nsingle=$($CLI -p $PNS info 2>/dev/null | grep -o 'net_driver:[a-z]*') nreactor=$($CLI -p $PNR info 2>/dev/null | grep -o 'net_driver:[a-z]*')"
  rss_snap launch
  return 0
}

stop_servers() {
  local p
  for p in $OSPID $ORPID $NSPID $NRPID $RPID $VPID; do kill $p 2>/dev/null; done
  for p in $OSPID $ORPID $NSPID $NRPID $RPID $VPID; do wait $p 2>/dev/null; done
  for p in $POS $POR $PNS $PNR $PR $PV; do port_closed_wait $p; done
  local arm dir
  for arm in OS OR NS NR; do
    eval dir=\$DIR_$arm
    if [ -f "$dir/f3srv.log" ] && [ "$(wc -l < $dir/f3srv.log)" -gt 1 ]; then
      cp $dir/f3srv.log "$G/cells/$CELL.f3srv.$arm.log"
    fi
  done
  rm -rf "$DIR_OS" "$DIR_OR" "$DIR_NS" "$DIR_NR" "$DIR_R" "$DIR_V"
  OSPID=""; ORPID=""; NSPID=""; NRPID=""; RPID=""; VPID=""
}

check_alive() { # arm tag
  local pid; pid=$(pid_of "$1")
  if ! kill -0 $pid 2>/dev/null || ! $CLI -p "$(port_of "$1")" ping >/dev/null 2>&1; then
    meta "CRASH $1 dead after $2"
    return 1
  fi
  return 0
}

ab_run() { # arm rep flags...
  local arm=$1 rep=$2; shift 2
  local out="$G/cells/$CELL.ab.$arm.rep$rep"
  taskset -c $CMASK $AB -aki-addr 127.0.0.1:"$(port_of "$arm")" \
    -redis-addr 127.0.0.1:$PR -valkey-addr 127.0.0.1:$PV \
    -cpu-server $SMASK -cpu-client $CMASK \
    -warm $WARM "$@" -json "$out.json" > "$out.out" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && [ $rc -ne 2 ] && meta "ab $arm rep$rep exit=$rc"
  check_alive "$arm" "ab rep$rep" || return 1
  return 0
}

rb_one() { # target rep n args...
  local tgt=$1 rep=$2 n=$3; shift 3
  taskset -c $CMASK $RB -p "$(port_of "$tgt")" -n "$n" --csv "$@" \
    > "$G/cells/$CELL.rb.$tgt.rep$rep.csv" 2>"$G/cells/$CELL.rb.$tgt.rep$rep.err"
  check_alive "$tgt" "rb rep$rep" || return 1
  return 0
}

rb_preload() { # target d
  taskset -c $CMASK $RB -p "$(port_of "$1")" -t set -d "$2" -r $KEYS -n 4000000 -c 64 -P 64 --threads 4 -q >/dev/null 2>&1
}

# cell: $1 wl $2 vtok $3 vbytes $4 pipeline $5 conns $6 rbn $7 arena-mib
matrix_cell() {
  local wl=$1 vtok=$2 vbytes=$3 pipe=$4 conns=$5 rbn=$6 arena=$7
  start_servers "$arena" || { stop_servers; return; }
  meta "cell wl=$wl value=$vtok keys=$KEYS dist=uniform conns=$conns pipeline=$pipe arena_mib_per_shard=$arena warm=$WARM dur=8s"
  local abf="-workload $wl -value-size $vtok -keys $KEYS -dist uniform -connections $conns -pipeline $pipe -duration 8s"
  local r arm
  for r in 0 1 2; do
    for arm in osingle oreactor nsingle nreactor; do
      flush_all
      ab_run "$arm" $r $abf || { stop_servers; return; }
      rss_snap "post-ab-$arm-rep$r"
    done
  done
  for arm in osingle oreactor nsingle nreactor redis valkey; do
    flush_all
    if [ "$wl" = get ]; then rb_preload "$arm" "$vbytes"; fi
    for r in 0 1 2; do
      rb_one "$arm" $r "$rbn" -t "$wl" -d "$vbytes" -r $KEYS -c "$conns" -P "$pipe" --threads 4 || { stop_servers; return; }
    done
    rss_snap "post-rb-$arm"
  done
  meta "dbsize_end osingle=$($CLI -p $POS dbsize) nsingle=$($CLI -p $PNS dbsize) redis=$($CLI -p $PR dbsize)"
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
  echo "f3srv-old sha256: $(sha256sum $F3OLD)"
  echo "f3srv-new sha256: $(sha256sum $F3NEW)"
  echo "net_loops: $NL"
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  $RB --version
  free -h
  uptime
} > $G/env.txt 2>&1

log "rss matrix starting; NL=$NL"
pkill -f "f3srv" 2>/dev/null; sleep 1

# lab 20 A/B, both arms, both value shapes, before the server matrix.
if ! grep -qxF lab20 $G/done.list 2>/dev/null; then
  log "lab20 start"
  for arm in old new; do
    for v in 64 1024; do
      taskset -c $SMASK /root/f3gate/rss-ab/bin/lab20-$arm -val $v > $G/lab20.$arm.v$v.txt 2>&1
    done
  done
  echo lab20 >> $G/done.list
  log "lab20 end"
fi

#                                   wl  vtok vbytes P  conns rbn      arena
run_cell get_64b_p16_c512 matrix_cell get 64  64   16 512  20000000 512
run_cell set_64b_p16_c512 matrix_cell set 64  64   16 512  20000000 512
run_cell get_1k_p16_c512  matrix_cell get 1k  1024 16 512  10000000 1024
run_cell set_1k_p16_c512  matrix_cell set 1k  1024 16 512  10000000 1024
run_cell get_64b_p1_c512  matrix_cell get 64  64   1  512  3000000  512
run_cell set_64b_p1_c512  matrix_cell set 64  64   1  512  3000000  512

log "rss matrix complete: $(wc -l < $G/done.list) cells done"
