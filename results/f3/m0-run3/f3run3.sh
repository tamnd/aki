#!/bin/bash
# f3 M0 run3 (issue tamnd/aki#542). First run with honest write cardinality
# (aki-bench #42 distinct key streams per connection, distinct_keys_est in rows)
# and aki #573 hot-set LTM residency. Protocol matches m0-rerun: -warm 3s,
# 3 timed windows (8s std, 20s LTM), FLUSHALL between reps on all servers,
# aki-bench and redis-benchmark --threads 4 both recorded, server cpus 0-7,
# generator 8-15, io-threads 4. Measurement only; no repo code changes.
# Resumable via $G/done.list.
set -u
export PATH=/root/bin:/usr/local/go/bin:/usr/bin:/bin:/usr/sbin:/sbin

G=/root/f3gate/m0-run3
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

start_sampler() { # 1s VmRSS sampler for all three servers, kB
  (
    while :; do
      echo "$(date -u +%s) aki=$(awk '/VmRSS/{print $2}' /proc/$APID/status 2>/dev/null) redis=$(awk '/VmRSS/{print $2}' /proc/$RPID/status 2>/dev/null) valkey=$(awk '/VmRSS/{print $2}' /proc/$VPID/status 2>/dev/null)"
      sleep 1
    done
  ) >> "$G/cells/$CELL.rss.samples" 2>/dev/null &
  SAMPLER=$!
}

stop_sampler() {
  [ -n "$SAMPLER" ] && kill $SAMPLER 2>/dev/null
  SAMPLER=""
}

start_servers() { # $1 extra f3srv args, $2 rival extra args (optional)
  local aextra="$1" rextra="${2:-}"
  DIR_A=$(mktemp -d $G/tmp/a.XXXXXX)
  DIR_R=$(mktemp -d $G/tmp/r.XXXXXX)
  DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PA $aextra >$DIR_A/f3srv.log 2>&1 &
  APID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $RT --dir $DIR_R $rextra >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $VT --io-threads-do-reads yes --dir $DIR_V $rextra >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  wait_ping $PA || { meta "FATAL f3srv did not answer PING"; cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log; return 1; }
  wait_ping $PR || { meta "FATAL redis did not answer PING"; return 1; }
  wait_ping $PV || { meta "FATAL valkey did not answer PING"; return 1; }
  meta "cf2_pins aki=$(taskset -cp $APID 2>&1) redis=$(taskset -cp $RPID 2>&1) valkey=$(taskset -cp $VPID 2>&1)"
  meta "launch aki_extra='$aextra' rival_extra='$rextra' io_threads redis=$RT valkey=$VT warm=$WARM"
  meta "shards_banner $(grep -o 'with [0-9]* shards' $DIR_A/f3srv.log | head -1)"
  rss_snap launch
  return 0
}

stop_servers() {
  stop_sampler
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

rival_info_snap() { # tag: rivals' hit/miss/evicted counters
  local tag=$1 p name
  for name in redis:$PR valkey:$PV; do
    p=${name#*:}
    echo "rival_info[$tag][${name%%:*}] $($CLI -p $p info stats 2>/dev/null | grep -E '^(keyspace_hits|keyspace_misses|evicted_keys):' | tr -d '\r' | tr '\n' ' ')" >> "$G/cells/$CELL.meta"
  done
}

ab_run() { # rep flags...
  local rep=$1; shift
  local out="$G/cells/$CELL.ab.rep$rep"
  taskset -c $CMASK $AB -aki-addr 127.0.0.1:$PA -redis-addr 127.0.0.1:$PR -valkey-addr 127.0.0.1:$PV \
    -cpu-server $SMASK -cpu-client $CMASK \
    -warm $WARM "$@" -json "$out.json" > "$out.out" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && [ $rc -ne 2 ] && meta "ab rep$rep exit=$rc"
  if grep -qi "keyspace coverage" "$out.out" 2>/dev/null; then
    meta "COVERAGE-NOTE rep$rep: $(grep -i 'keyspace coverage' $out.out | tr '\n' '; ')"
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

# std_cell: same shape as m0-rerun.
#   $1 workload $2 value-tok $3 value-bytes $4 keys $5 dist $6 conns
#   $7 rb-kind (set|get|incr|none) $8 rb-n $9 arena-mib $10 ab-duration
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

# ltm_cell: larger-than-memory strings per scenarios/f3-ltm-strings protocol,
# adapted to the gate box split. ~2GB dataset (2M x 1032B), aki 4 shards x
# 128MiB resident cap (=512MiB) + vlog on disk, rivals --maxmemory 512mb
# --maxmemory-policy allkeys-lfu. aki-bench is the sole harness (rb counts
# rival nils as ops, which is the exact bug this scenario exists to fix).
# 3 reps of warm 3s + 20s windows, FLUSHALL between reps (aki-bench re-preloads).
#   $1 workload (get|set)  $2 dist (uniform|zipfian)
ltm_cell() {
  local wl=$1 dist=$2
  local n=2000000 vbytes=1032 conns=64 shards=4 arena=256 rescap=128
  DIR_A=$(mktemp -d $G/tmp/a.XXXXXX)  # pre-make so vlog dir exists at launch
  local vlog=$DIR_A/vlog; mkdir -p "$vlog"
  DIR_R=$(mktemp -d $G/tmp/r.XXXXXX)
  DIR_V=$(mktemp -d $G/tmp/v.XXXXXX)
  taskset -c $SMASK $F3SRV --addr 127.0.0.1:$PA --shards $shards \
    --arena-mib $arena --resident-cap-mib $rescap --vlog-dir "$vlog" >$DIR_A/f3srv.log 2>&1 &
  APID=$!
  taskset -c $SMASK /root/bin/redis-server --port $PR --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $RT --dir $DIR_R \
    --maxmemory 512mb --maxmemory-policy allkeys-lfu >$DIR_R/redis.log 2>&1 &
  RPID=$!
  taskset -c $SMASK /root/bin/valkey-server --port $PV --bind 127.0.0.1 --save "" \
    --appendonly no --io-threads $VT --io-threads-do-reads yes --dir $DIR_V \
    --maxmemory 512mb --maxmemory-policy allkeys-lfu >$DIR_V/valkey.log 2>&1 &
  VPID=$!
  wait_ping $PA || { meta "FATAL f3srv did not answer PING"; cp $DIR_A/f3srv.log $G/cells/$CELL.f3srv.log; stop_servers; return; }
  wait_ping $PR || { meta "FATAL redis did not answer PING"; stop_servers; return; }
  wait_ping $PV || { meta "FATAL valkey did not answer PING"; stop_servers; return; }
  meta "cell LTM wl=$wl dist=$dist keys=$n value=${vbytes}B conns=$conns pipeline=16 warm=$WARM dur=20s"
  meta "aki posture: shards=$shards arena_mib=$arena resident_cap_mib=$rescap (total cap 512MiB) vlog=$vlog"
  meta "rival posture: --maxmemory 512mb --maxmemory-policy allkeys-lfu io-threads=4"
  meta "vlog_fs $(stat -f -c %T $vlog)"
  rss_snap launch
  start_sampler
  local abf="-workload $wl -value-size $vbytes -keys $n -dist $dist -connections $conns -pipeline 16 -duration 20s"
  [ "$dist" = zipfian ] && abf="$abf -zipf-s 0.99"
  local r
  info_snap pre-rep0
  rival_info_snap pre-rep0
  for r in 0 1 2; do
    flush_all
    ab_run $r $abf || { stop_servers; return; }
    rss_snap post-rep$r
    info_snap post-rep$r
    rival_info_snap post-rep$r
  done
  rss_snap post-ab
  meta "rb: n/a (LTM eviction posture; rb counts rival nils as ops)"
  meta "vlog_du $(du -sh $vlog 2>/dev/null)"
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

{
  date
  uname -a
  echo "aki commit: $(cd /root/aki && git rev-parse HEAD)"
  echo "aki-bench commit: $(cd /root/f3gate/aki-bench && git rev-parse HEAD)"
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  $RB --version
  go version
  free -h
  uptime
} > $G/env.txt 2>&1

log "m0 run3 starting; aki $(cd /root/aki && git rev-parse --short HEAD), aki-bench $(cd /root/f3gate/aki-bench && git rev-parse --short HEAD)"
pkill -f "f3srv --addr 127.0.0.1:71" 2>/dev/null; sleep 1

#                              wl   vtok vbytes keys    dist    conns rbkind rbn      arena dur
run_cell set_64b_1m  std_cell set  64   64   1000000 uniform 512  set    20000000 512  8s
run_cell incr_1m     std_cell incr 64   64   1000000 uniform 512  incr   20000000 512  8s
run_cell set_1k_1m   std_cell set  1k   1024 1000000 uniform 512  set    10000000 1024 8s
run_cell get_64b_1m  std_cell get  64   64   1000000 uniform 512  get    20000000 512  8s
run_cell get_1k_1m   std_cell get  1k   1024 1000000 uniform 512  get    10000000 1024 8s

run_cell ltm_get_uniform ltm_cell get uniform
run_cell ltm_get_zipf    ltm_cell get zipfian
run_cell ltm_set_uniform ltm_cell set uniform

log "m0 run3 complete: $(wc -l < $G/done.list) cells done"
