#!/bin/sh
# Full frame-overhead sweep into frameoverhead.csv.
set -eu
cd "$(dirname "$0")"
go run . "$@" | tee frameoverhead.csv
