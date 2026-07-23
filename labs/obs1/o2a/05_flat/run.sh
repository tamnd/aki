#!/bin/sh
set -e
go run . | tee flat.csv
