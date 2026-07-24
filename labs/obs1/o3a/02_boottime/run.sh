#!/bin/sh
set -e
go run . | tee boot.csv
