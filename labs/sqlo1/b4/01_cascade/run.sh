#!/bin/sh
# Full cascade sweep. Writes CSV rows to stdout; redirect to cascade.csv.
set -e
cd "$(dirname "$0")"
go build -o /tmp/cascade .

echo "shape,n,workload,arg,ratio,enc_ns_val,dec_ns_val,x1,x2"

# Encoder pricing per shape: ratio against the raw framing, encode and
# decode ns per value, encoded bytes. x1 = applicable, x2 = bytes.
for shape in counters timestamps u64s uuids json mixed; do
  /tmp/cascade -shape $shape -n 200000 -workload encoder
done

# Floor sweep: the selection rule over 256-value groups, total ratio
# and weighted decode ns. x1 = lightweight share pct, x2 = zstd share.
for shape in counters timestamps u64s uuids json mixed; do
  for floor in 0.00 0.04 0.08 0.12 0.16 0.24; do
    /tmp/cascade -shape $shape -n 200000 -workload select -floor $floor
  done
done

# Group size sensitivity at the spec floor.
for gsize in 64 256 1024 4096; do
  /tmp/cascade -shape mixed -n 200000 -workload select -floor 0.08 -gsize $gsize
done

# Sampled selector against the full-group pick at the spec floor.
# x1 = match pct, x2 = byte penalty pct versus the oracle selection.
for shape in counters timestamps u64s uuids json mixed; do
  for rate in 0.01 0.05 0.20; do
    /tmp/cascade -shape $shape -n 200000 -workload sample -floor 0.08 -rate $rate
  done
done
