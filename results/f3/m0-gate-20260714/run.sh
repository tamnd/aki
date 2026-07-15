#!/bin/bash
# M0 headline gate: P16/c512, 64-byte values, 1M uniform keys.
# Two concurrent redis-benchmark processes avoid the random-key generator cap.
set -euo pipefail

ROOT=${ROOT:-/root/f3gate/m0-gate-20260714}
SOURCE_COMMIT=${SOURCE_COMMIT:-unknown}
SRC=$ROOT/src
BIN=$ROOT/bin
RAW=$ROOT/raw
RB=/root/bin/redis-benchmark
CLI=/root/bin/redis-cli
SMASK=4-17
CMASK1=18-24
CMASK2=25-31
PORT=7411
N=10000000
PRELOAD_N=2500000

mkdir -p "$BIN" "$RAW"

wait_ping() {
  for _ in $(seq 1 100); do
    "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

stop_server() {
  if [ -n "${SPID:-}" ]; then
    kill "$SPID" 2>/dev/null || true
    wait "$SPID" 2>/dev/null || true
    SPID=
  fi
}
trap stop_server EXIT

start_server() {
  local target=$1 workload=$2 dir="$ROOT/tmp/$target-$workload"
  rm -rf "$dir"
  mkdir -p "$dir"
  case "$target" in
    aki)
      GOMAXPROCS=14 taskset -c "$SMASK" "$BIN/f3srv" \
        -addr "127.0.0.1:$PORT" -shards 8 -arena-mib 512 \
        -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 \
        >"$RAW/$target-$workload.server.log" 2>&1 &
      ;;
    redis)
      taskset -c "$SMASK" /root/bin/redis-server --port "$PORT" \
        --bind 127.0.0.1 --save '' --appendonly no --io-threads 6 \
        --dir "$dir" >"$RAW/$target-$workload.server.log" 2>&1 &
      ;;
    valkey)
      taskset -c "$SMASK" /root/bin/valkey-server --port "$PORT" \
        --bind 127.0.0.1 --save '' --appendonly no --io-threads 4 \
        --io-threads-do-reads yes --dir "$dir" \
        >"$RAW/$target-$workload.server.log" 2>&1 &
      ;;
  esac
  SPID=$!
  wait_ping
  taskset -cp "$SPID" >"$RAW/$target-$workload.affinity.txt" 2>&1
}

dual_run() {
  local target=$1 workload=$2 rep=$3 prefix="$RAW/$target-$workload-rep$rep"
  taskset -c "$CMASK1" "$RB" -p "$PORT" -t "$workload" -d 64 \
    -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv \
    >"$prefix-a.csv" 2>"$prefix-a.err" &
  local p1=$!
  taskset -c "$CMASK2" "$RB" -p "$PORT" -t "$workload" -d 64 \
    -r 1000000 -n "$N" -c 256 -P 16 --threads 7 --csv \
    >"$prefix-b.csv" 2>"$prefix-b.err" &
  local p2=$!
  wait "$p1"
  wait "$p2"
}

preload() {
  local target=$1 rep=$2 prefix="$RAW/$target-get-preload-rep$rep"
  taskset -c "$CMASK1" "$RB" -p "$PORT" -t set -d 64 -r 1000000 \
    -n "$PRELOAD_N" -c 256 -P 16 --threads 7 --csv \
    >"$prefix-a.csv" 2>"$prefix-a.err" &
  local p1=$!
  taskset -c "$CMASK2" "$RB" -p "$PORT" -t set -d 64 -r 1000000 \
    -n "$PRELOAD_N" -c 256 -P 16 --threads 7 --csv \
    >"$prefix-b.csv" 2>"$prefix-b.err" &
  local p2=$!
  wait "$p1"
  wait "$p2"
}

snapshot() {
  local target=$1 workload=$2 rep=$3
  {
    echo "pid=$SPID"
    awk '/VmRSS|VmHWM/{print}' "/proc/$SPID/status"
    echo "dbsize=$($CLI -p "$PORT" dbsize)"
    "$CLI" -p "$PORT" info memory | sed -n '/^used_memory:/p'
  } >"$RAW/$target-$workload-rep$rep.memory.txt"
}

run_cell() {
  local target=$1 workload=$2 rep
  start_server "$target" "$workload"
  for rep in warm 1 2 3; do
    "$CLI" -p "$PORT" flushall >/dev/null
    if [ "$workload" = get ]; then
      preload "$target" "$rep"
      snapshot "$target" "$workload-preload" "$rep"
    fi
    dual_run "$target" "$workload" "$rep"
    snapshot "$target" "$workload" "$rep"
  done
  stop_server
}

{
  date -u +'%Y-%m-%dT%H:%M:%SZ'
  uname -a
  nproc
  free -h
  go version
  echo "source_commit=$SOURCE_COMMIT"
  /root/bin/redis-server --version
  /root/bin/valkey-server --version
  /root/bin/redis-benchmark --version
} >"$ROOT/env.txt"

go -C "$SRC" test ./engine/f3/... ./f3srv/... ./cmd/f3srv/...
go -C "$SRC" build -trimpath -o "$BIN/f3srv" ./cmd/f3srv
sha256sum "$BIN/f3srv" /root/bin/redis-server /root/bin/valkey-server \
  /root/bin/redis-benchmark >"$ROOT/binaries.sha256"

for target in aki redis valkey; do
  run_cell "$target" set
  run_cell "$target" get
done

python3 - "$RAW" >"$ROOT/summary.tsv" <<'PY'
import csv
import pathlib
import statistics
import sys

raw = pathlib.Path(sys.argv[1])
print("target\tworkload\trep1\trep2\trep3\tmedian_ops_s\tworst_p99_ms")
for target in ("aki", "redis", "valkey"):
    for workload in ("set", "get"):
        totals = []
        p99s = []
        for rep in (1, 2, 3):
            rates = []
            client_p99 = []
            for side in ("a", "b"):
                path = raw / f"{target}-{workload}-rep{rep}-{side}.csv"
                rows = list(csv.reader(path.open()))
                row = next(r for r in rows if r and r[0].strip('"').lower() == workload)
                rates.append(float(row[1]))
                client_p99.append(float(row[6]))
            totals.append(sum(rates))
            p99s.append(max(client_p99))
        print(f"{target}\t{workload}\t" + "\t".join(f"{x:.0f}" for x in totals) +
              f"\t{statistics.median(totals):.0f}\t{max(p99s):.3f}")
PY

touch "$ROOT/complete"
