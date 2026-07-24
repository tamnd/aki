#!/bin/sh
set -e
go run . | tee k2chain.csv
