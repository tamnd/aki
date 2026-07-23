#!/bin/sh
# Full ringpool sweep. Writes CSV rows to stdout; redirect to ringpool.csv.
set -e
cd "$(dirname "$0")"
go build -o /tmp/ringpool .

echo "backend,workload,depth,batch,n,secs,ops_s,mb_s,p50_us,p99_us"

backends="iopool"
if /tmp/ringpool -backend ring -probe 2>/dev/null; then
  backends="iopool ring"
else
  echo "ring arm skipped: io_uring unavailable here" >&2
fi

# Depth by batch, both backends, both shapes. Batch 128 stays in the
# sweep to price the tail the spec bans it for.
for backend in $backends; do
  for depth in 2 4 8 16 32; do
    for batch in 1 4 8 16 32 64 128; do
      /tmp/ringpool -backend $backend -workload coldread -depth $depth -batch $batch -n 20000
      /tmp/ringpool -backend $backend -workload drain -depth $depth -batch $batch -n 16384
    done
  done
done
