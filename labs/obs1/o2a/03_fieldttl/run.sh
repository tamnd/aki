#!/bin/sh
set -e
go run . | tee fieldttl.csv
