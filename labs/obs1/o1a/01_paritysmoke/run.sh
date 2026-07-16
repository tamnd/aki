#!/bin/sh
# Full parity sweep: build both binaries from this tree, run the smoke,
# leave the rows in paritysmoke.csv.
set -eu
cd "$(dirname "$0")"
root=$(cd ../../../.. && pwd)
mkdir -p bin
go build -o bin/f3srv "$root/cmd/f3srv"
go build -o bin/obs1srv "$root/cmd/obs1srv"
go run . "$@" | tee paritysmoke.csv
