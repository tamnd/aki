#!/bin/sh
# Full zdict sweep. Writes CSV rows to stdout; redirect to zdict.csv.
set -e
cd "$(dirname "$0")"
go build -o /tmp/zdict .

echo "shape,n,workload,arg,ratio,enc_ns_val,dec_ns_val,x1,x2"

# Dictionary size sweep at 100x training, 64-value groups.
# x1 = plain zstd ratio, x2 = dict win pct over plain.
for shape in json sess user rand; do
  for kib in 16 48 112 224; do
    /tmp/zdict -shape $shape -workload dictsize -dict $kib
  done
done

# Group size sweep at the 112 KiB dictionary: where plain zstd's own
# context catches up and the dictionary stops paying.
for shape in json sess user; do
  for g in 4 16 64 256 1024 4096; do
    /tmp/zdict -shape $shape -workload gsize -dict 112 -gsize $g
  done
done

# Training volume sweep at 112 KiB: bytes sampled as a multiple of the
# dictionary size.
for shape in json sess user; do
  for tx in 10 30 100 300; do
    /tmp/zdict -shape $shape -workload train -dict 112 -trainx $tx
  done
done

# Workload shift: incumbent trained on template set 1, corpus drifts to
# set 2. ratio = incumbent on drifted data, x1 = plain, x2 = fresh
# candidate's held-out win over the incumbent (the 5 percent trigger).
for shape in json sess user; do
  for p in 0.00 0.25 0.50 0.75 1.00; do
    /tmp/zdict -shape $shape -workload shift -dict 112 -drift $p
  done
done
