#!/bin/sh
set -e
go run . | tee gated3.csv
