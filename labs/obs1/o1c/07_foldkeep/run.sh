#!/bin/sh -e
cd "$(dirname "$0")"
echo "== paced at design ingest, 100 MiB/s =="
go run . | tee foldkeep-paced.csv
echo "== unpaced contrast arm =="
go run . -mibs 0 | tee foldkeep-unpaced.csv
