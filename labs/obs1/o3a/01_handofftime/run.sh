#!/bin/sh
set -e
go run . | tee handoff.csv
