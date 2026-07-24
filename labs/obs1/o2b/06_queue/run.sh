#!/bin/sh
set -e
go run . | tee queue.csv
