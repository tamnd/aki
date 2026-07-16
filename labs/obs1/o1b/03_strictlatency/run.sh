#!/bin/sh
# Full strict-latency sweep into strictlatency.csv.
set -eu
cd "$(dirname "$0")"
go run . "$@" | tee strictlatency.csv
