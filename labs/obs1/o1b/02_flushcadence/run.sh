#!/bin/sh
# Full flush-cadence sweep into flushcadence.csv.
set -eu
cd "$(dirname "$0")"
go run . "$@" | tee flushcadence.csv
