#!/bin/sh
set -e
go run . | tee foldload.csv
