#!/usr/bin/env bash
# M6 point-op gate suite: start the three servers once, ping them ready, then
# drive every op through a dual redis-benchmark generator (median of 3), print
# one row per op, kill once. Servers stay up across ops so there is no per-op
# rebind race. Run from a shell whose command line does NOT name the servers.
set -u

PORT_AKI=7001; PORT_RED=7002; PORT_VAL=7003
REQS=1000000; PIPE=16; CLIENTS=50
BIN=/root/bin
FS="$BIN/f3srv"; RS="$BIN/redis-server"; VS="$BIN/valkey-server"
RB="$BIN/redis-benchmark"; CLI="$BIN/redis-cli"

GOGC=20 GOMAXPROCS=14 taskset -c 4-17 "$FS" -addr 127.0.0.1:$PORT_AKI -shards 8 \
  -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -rep-cap 512 \
  -net reactor -net-loops 0 </dev/null >/tmp/aki6.log 2>&1 &
pA=$!
rm -rf /tmp/red6dir; mkdir -p /tmp/red6dir
taskset -c 4-17 "$RS" --port $PORT_RED --dir /tmp/red6dir --save '' --appendonly no \
  --io-threads 6 </dev/null >/tmp/red6.log 2>&1 &
pR=$!
rm -rf /tmp/val6dir; mkdir -p /tmp/val6dir
taskset -c 4-17 "$VS" --port $PORT_VAL --dir /tmp/val6dir --save '' --appendonly no \
  --io-threads 4 </dev/null >/tmp/val6.log 2>&1 &
pV=$!

waitup() { # port
  for _ in $(seq 1 50); do
    if [ "$("$CLI" -p "$1" ping 2>/dev/null)" = "PONG" ]; then return 0; fi
    sleep 0.2
  done
  echo "SERVER on $1 never came up" >&2; return 1
}
waitup $PORT_AKI; waitup $PORT_RED; waitup $PORT_VAL

gen() { # port cmd...
  local port=$1; shift
  taskset -c 18-24 "$RB" -h 127.0.0.1 -p "$port" -n $REQS -P $PIPE -c $CLIENTS -q "$@" \
    >/tmp/g1.txt 2>&1 &
  local a=$!
  taskset -c 25-31 "$RB" -h 127.0.0.1 -p "$port" -n $REQS -P $PIPE -c $CLIENTS -q "$@" \
    >/tmp/g2.txt 2>&1 &
  local b=$!
  wait $a; wait $b
  local r1 r2
  r1=$(grep -oE '[0-9]+\.[0-9]+ requests' /tmp/g1.txt | head -1 | grep -oE '^[0-9]+')
  r2=$(grep -oE '[0-9]+\.[0-9]+ requests' /tmp/g2.txt | head -1 | grep -oE '^[0-9]+')
  echo $(( ${r1:-0} + ${r2:-0} ))
}
med() { printf '%s\n' "$@" | sort -n | sed -n '2p'; }

runop() { # label cmd...
  local label=$1; shift
  local a1 a2 a3 r1 r2 r3 v1 v2 v3
  a1=$(gen $PORT_AKI "$@"); r1=$(gen $PORT_RED "$@"); v1=$(gen $PORT_VAL "$@")
  a2=$(gen $PORT_AKI "$@"); r2=$(gen $PORT_RED "$@"); v2=$(gen $PORT_VAL "$@")
  a3=$(gen $PORT_AKI "$@"); r3=$(gen $PORT_RED "$@"); v3=$(gen $PORT_VAL "$@")
  local ak re va
  ak=$(med $a1 $a2 $a3); re=$(med $r1 $r2 $r3); va=$(med $v1 $v2 $v3)
  local rr rv mn
  rr=$(awk "BEGIN{printf \"%.2f\", $ak/($re==0?1:$re)}")
  rv=$(awk "BEGIN{printf \"%.2f\", $ak/($va==0?1:$va)}")
  mn=$(awk "BEGIN{m=($rr<$rv)?$rr:$rv; printf \"%.2f\", m}")
  printf '%-28s aki=%-8s redis=%-8s valkey=%-8s r=%s v=%s min=%s\n' \
    "$label" "$ak" "$re" "$va" "$rr" "$rv" "$mn"
}

OUT=/tmp/m6suite.txt; : > $OUT
{
  runop "SETBIT"    setbit bitk 100 1
  runop "GETBIT"    getbit bitk 100
  runop "BITCOUNT"  bitcount bitk
  runop "BITPOS"    bitpos bitk 1
  runop "BITFIELD"  bitfield bitk incrby u8 0 1
  runop "PFADD"     pfadd hllk e1 e2 e3
  runop "PFCOUNT"   pfcount hllk
} | tee -a $OUT

kill $pA $pR $pV 2>/dev/null; wait 2>/dev/null
echo SUITE_DONE
