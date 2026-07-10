#!/bin/bash
# f3 io_uring A/B matrix (M10 slice 3). Adapted from the reactor campaign's
# f3matrix.sh: three aki arms resident at once (goroutine-single 7111,
# reactor 7115, uring 7116) plus redis 7112 and valkey 7113, all taskset 0-7;
# generator 8-15. Arms alternate within each rep (same-session interleaving)
# and every aki-bench invocation measures its arm AND both rivals, so per-arm
# ratios are same-invocation. Protocol per m0-run3: warm 3s, 3 timed 8s
# windows, none discarded, FLUSHALL between reps on all servers, 1M keys
# uniform, redis-benchmark --threads 4 with -P matching the pipeline.
# Resumable via $G/done.list.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/uring-ab/matrix
F3SRV=/root/f3gate/uring-ab/bin/f3srv
AB=/root/f3gate/uring-ab/bin/aki-bench
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=0-7
CMASK=8-15
PS=7111; PR=7112; PV=7113; PX=7115; PU=7116
NL=${NL:-3}   # loops for both event-loop arms, lab 19's frozen 2/5 share
WARM=3s
KEYS=1000000

SPID=""; RPID=""; VPID=""; XPID=""; UPID=""
DIR_S=""; DIR_R=""; DIR_V=""; DIR_X=""; DIR_U=""
CELL=""

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
meta() { echo "$*" >> "$G/cells/$CELL.meta"; }

port_of() { case $1 in single) echo $PS;; reactor) echo $PX;; uring) echo $PU;; redis) echo $PR;; valkey) echo $PV;; esac; }
pid_of() { case $1 in single) echo $SPID;; reactor) echo $XPID;; uring) echo $UPID;; redis) echo $RPID;; valkey) echo $VPID;; esac; }

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
  for p in $PS $PX $PU $PR $PV; do $CLI -p $p flushall >/dev/null 2>&1; done
  sleep 0.5
}

rss_snap() { # tag
  meta "rss[$1] single=$(awk '/VmRSS/{print $2$3}' /proc/$SPID/status 2>/dev/null) reactor=$(awk '/VmRSS/{print $2$3}' /proc/$XPID/status 2>/dev/null) uring=$(awk '/VmRSS/{print $2$3}' /proc/$UPID/status 2>/dev/null) redis=$(awk '/VmRSS/{print $2$3}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmRSS/{print $2$3}' /proc/$VPID/status 2>/dev/null)"
}

start_servers() { # $1 arena-mib
  local arena=$1
  DIR_S=$(mktemp -d $G/tmp/s.XXXXXX); DIR_X=$(mktemp -d $G/tmp/x.XXXXXX)
  DIR_U=$(mktemp -d $G/tmp/u.XXXXXX); DIR_R=$(mktemp -d $G/tmp/r.XXXXXX)
  DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PS --arena-mib $arena >$DIR_S/f3srv.log 2>&1 &
  SPID=$!
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PX --arena-mib $arena -net reactor -net-loops $NL >$DIR_X/f3srv.log 2>&1 &
  XPID=$!
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PU --arena-mib $arena -net uring -net-loops $NL >$DIR_U/f3srv.log 2>&1 &
  UPID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads 4 --dir $DIR_R >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads 4 --io-threads-do-reads yes --dir $DIR_V >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  local p
  for p in $PS $PX $PU $PR $PV; do
    wait_ping $p || { meta "FATAL port $p did not answer PING"; return 1; }
  done
  meta "pins single=$(taskset -cp $SPID 2>&1) uring=$(taskset -cp $UPID 2>&1) redis=$(taskset -cp $RPID 2>&1)"
  meta "launch arena_mib=$arena net_loops=$NL warm=$WARM io_threads=4"
  meta "driver_stamp single=$($CLI -p $PS info 2>/dev/null | grep -o 'net_driver:[a-z]*') reactor=$($CLI -p $PX info 2>/dev/null | grep -o 'net_driver:[a-z]*') uring=$($CLI -p $PU info 2>/dev/null | grep -o 'net_driver:[a-z]*')"
  rss_snap launch
  return 0
}

stop_servers() {
  local p
  for p in $SPID $XPID $UPID $RPID $VPID; do kill $p 2>/dev/null; done
  for p in $SPID $XPID $UPID $RPID $VPID; do wait $p 2>/dev/null; done
  for p in $PS $PX $PU $PR $PV; do port_closed_wait $p; done
  local arm dir
  for arm in S X U; do
    eval dir=\$DIR_$arm
    if [ -f "$dir/f3srv.log" ] && [ "$(wc -l < $dir/f3srv.log)" -gt 1 ]; then
      cp $dir/f3srv.log "$G/cells/$CELL.f3srv.$arm.log"
    fi
  done
  rm -rf "$DIR_S" "$DIR_X" "$DIR_U" "$DIR_R" "$DIR_V"
  SPID=""; XPID=""; UPID=""; RPID=""; VPID=""
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
  if grep -qi "keyspace coverage" "$out.out" 2>/dev/null; then
    meta "COVERAGE-NOTE $arm rep$rep: $(grep -i 'keyspace coverage' $out.out | tr '\n' '; ')"
  fi
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
    for arm in single reactor uring; do
      flush_all
      ab_run "$arm" $r $abf || { stop_servers; return; }
      rss_snap "post-ab-$arm-rep$r"
    done
  done
  local rbwl=$wl
  for arm in single reactor uring redis valkey; do
    flush_all
    if [ "$wl" = get ]; then rb_preload "$arm" "$vbytes"; fi
    for r in 0 1 2; do
      rb_one "$arm" $r "$rbn" -t "$rbwl" -d "$vbytes" -r $KEYS -c "$conns" -P "$pipe" --threads 4 || { stop_servers; return; }
    done
    rss_snap "post-rb-$arm"
  done
  meta "dbsize_end single=$($CLI -p $PS dbsize) uring=$($CLI -p $PU dbsize) redis=$($CLI -p $PR dbsize)"
  meta "uring_stats $($CLI -p $PU info 2>/dev/null | grep -o 'net_[a-z_]*:[0-9a-z]*' | tr '\n' ' ')"
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
  echo "f3srv sha256: $(sha256sum $F3SRV)"
  echo "net_loops: $NL"
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  $RB --version
  go version
  free -h
  uptime
} > $G/env.txt 2>&1

log "matrix starting; aki $(cd /root/aki && git rev-parse --short HEAD), NL=$NL"
pkill -f "f3srv --addr 127.0.0.1:71" 2>/dev/null; sleep 1

# The lever cells and the gate block run first so an interrupted session
# still answers the section 5.4 question.
#                                   wl  vtok vbytes P  conns rbn      arena
run_cell get_64b_p1_c512  matrix_cell get 64  64   1  512  3000000  512
run_cell set_64b_p1_c512  matrix_cell set 64  64   1  512  3000000  512
run_cell get_1k_p1_c512   matrix_cell get 1k  1024 1  512  3000000  1024
run_cell set_1k_p1_c512   matrix_cell set 1k  1024 1  512  3000000  1024
run_cell get_64b_p16_c512 matrix_cell get 64  64   16 512  20000000 512
run_cell set_64b_p16_c512 matrix_cell set 64  64   16 512  20000000 512
run_cell get_1k_p16_c512  matrix_cell get 1k  1024 16 512  10000000 1024
run_cell set_1k_p16_c512  matrix_cell set 1k  1024 16 512  10000000 1024
run_cell get_64b_p16_c50  matrix_cell get 64  64   16 50   20000000 512
run_cell set_64b_p16_c50  matrix_cell set 64  64   16 50   20000000 512
run_cell get_1k_p16_c50   matrix_cell get 1k  1024 16 50   10000000 1024
run_cell set_1k_p16_c50   matrix_cell set 1k  1024 16 50   10000000 1024
run_cell get_64b_p1_c50   matrix_cell get 64  64   1  50   1000000  512
run_cell set_64b_p1_c50   matrix_cell set 64  64   1  50   1000000  512
run_cell get_1k_p1_c50    matrix_cell get 1k  1024 1  50   1000000  1024
run_cell set_1k_p1_c50    matrix_cell set 1k  1024 1  50   1000000  1024

log "matrix complete: $(wc -l < $G/done.list) cells done"
