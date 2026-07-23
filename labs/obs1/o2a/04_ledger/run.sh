#!/bin/sh
set -e
go run . | tee ledger.csv
