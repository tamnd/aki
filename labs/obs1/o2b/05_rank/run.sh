#!/bin/sh
set -e
go run . | tee rank.csv
