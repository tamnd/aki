#!/usr/bin/env bash
# Sweep the IO unit on the target disk. Pass rule: 4 KiB is confirmed
# unless 16 KiB random reads land within ~10% of 4 KiB per-op time at
# the working depths and 16 KiB random writes show no amp penalty.
set -euo pipefail
cd "$(dirname "$0")"

dir=${1:-.}
out=unitsize.csv
go run . -dir "$dir" -filemb 8192 -secs 5 >"$out"
echo "wrote $out" >&2
