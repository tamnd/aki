#!/bin/sh
set -e
go run . | tee typepoint.csv
