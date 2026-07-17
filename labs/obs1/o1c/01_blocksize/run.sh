#!/bin/sh
# Full block-size sweep into blocksize.csv.
set -eu
cd "$(dirname "$0")"
go run . "$@" | tee blocksize.csv
