#!/bin/sh -e
cd "$(dirname "$0")"
command -v zstd >/dev/null || { echo "zstd binary required for the scored sweep" >&2; exit 1; }
go run . | tee zstdworth.csv
