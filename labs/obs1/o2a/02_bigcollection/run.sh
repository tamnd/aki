#!/bin/sh
set -e
go run . | tee bigcollection.csv
