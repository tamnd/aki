#!/bin/sh -e
cd "$(dirname "$0")"
go run . | tee foldthroughput.csv
