#!/usr/bin/env bash
# M6 bitmap point-op gate via redis-benchmark dual generator.
# Args: CMD... (a full redis-benchmark trailing command, e.g. setbit bitk 100 1)
# Prints one median-of-3 row: aki redis valkey ratio(min/rival).
set -u

CMD="$*"
PORT_AKI=7001
PORT_RED=7002
PORT_VAL=7003
REQS=1000000
PIPE=16
CLIENTS=50

BIN=/root/bin
FS="$BIN/f3srv"
RS="$BIN/redis-server"
VS="$BIN/valkey-server"
RB="$BIN/redis-benchmark"

start_aki() {
  GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$FS" -addr 127.0.0.1:$PORT_AKI -shards 8 \
    -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 \
    -net reactor -net-loops 0 </dev/null >/tmp/aki6.log 2>&1 &
  echo $!
}
start_red() {
  rm -rf /tmp/red6dir && mkdir -p /tmp/red6dir
  taskset -c 4-17 "$RS" --port $PORT_RED --dir /tmp/red6dir --save '' --appendonly no --io-threads 6 \
    </dev/null >/tmp/red6.log 2>&1 &
  echo $!
}
start_val() {
  rm -rf /tmp/val6dir && mkdir -p /tmp/val6dir
  taskset -c 4-17 "$VS" --port $PORT_VAL --dir /tmp/val6dir --save '' --appendonly no --io-threads 4 \
    </dev/null >/tmp/val6.log 2>&1 &
  echo $!
}

# dualgen PORT: run two redis-benchmark generators bound to disjoint core sets,
# each firing the command, and sum their throughput. Surfaces the true reactor
# keyspace ceiling that a single client-capped generator hides on O(1) ops.
dualgen() {
  local port=$1
  taskset -c 18-24 "$RB" -h 127.0.0.1 -p "$port" -n $REQS -P $PIPE -c $CLIENTS -q \
    $CMD >/tmp/g1.txt 2>&1 &
  local a=$!
  taskset -c 25-31 "$RB" -h 127.0.0.1 -p "$port" -n $REQS -P $PIPE -c $CLIENTS -q \
    $CMD >/tmp/g2.txt 2>&1 &
  local b=$!
  wait $a; wait $b
  # redis-benchmark -q line: "CMD: NNNN.NN requests per second..." grab both.
  local r1 r2
  r1=$(grep -oE '[0-9]+\.[0-9]+ requests' /tmp/g1.txt | head -1 | grep -oE '^[0-9]+')
  r2=$(grep -oE '[0-9]+\.[0-9]+ requests' /tmp/g2.txt | head -1 | grep -oE '^[0-9]+')
  echo $(( ${r1:-0} + ${r2:-0} ))
}

pA=$(start_aki); pR=$(start_red); pV=$(start_val)
sleep 2

med() { printf '%s\n' "$@" | sort -n | sed -n '2p'; }

declare -a AK RE VA
for i in 1 2 3; do
  AK[$i]=$(dualgen $PORT_AKI)
  RE[$i]=$(dualgen $PORT_RED)
  VA[$i]=$(dualgen $PORT_VAL)
done

ak=$(med ${AK[1]} ${AK[2]} ${AK[3]})
re=$(med ${RE[1]} ${RE[2]} ${RE[3]})
va=$(med ${VA[1]} ${VA[2]} ${VA[3]})

kill $pA $pR $pV 2>/dev/null
wait 2>/dev/null

rr=$(awk "BEGIN{printf \"%.2f\", $ak/$re}")
rv=$(awk "BEGIN{printf \"%.2f\", $ak/$va}")
mn=$(awk "BEGIN{print ($rr<$rv)?$rr:$rv}")
echo "CMD=[$CMD] aki=$ak redis=$re valkey=$va aki/redis=$rr aki/valkey=$rv min=$mn"
