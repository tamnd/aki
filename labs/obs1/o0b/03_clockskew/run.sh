#!/usr/bin/env bash
# Clock-skew sweep: constant offsets, rate skew, a frozen clock, and
# healthy no-partition arms. Deterministic and sim-only; the clock
# adversary is orthogonal to the store and fence-torture already
# covered store-level races on live MinIO.
set -euo pipefail
cd "$(dirname "$0")"

BIN=/tmp/obs1-clockskew
OUT=clockskew.csv
go build -o "$BIN" .
"$BIN" | tee "$OUT"
column -s, -t "$OUT"
