#!/usr/bin/env bash
# Fence-torture sweep: simulator arms across fault rates and fleet sizes,
# then a live MinIO arm when AKI_OBS1_S3 is set (export it plus optional
# AKI_OBS1_S3_USER/AKI_OBS1_S3_PASS; the bucket is created fresh per run).
# Any violation exits nonzero, so this script doubles as the gate.
set -euo pipefail
cd "$(dirname "$0")"

go build -o /tmp/obs1-fencetorture .
BIN=/tmp/obs1-fencetorture
OUT=fencetorture.csv

$BIN -header -schedules 0 > "$OUT"
echo "$($BIN -store sim -schedules 40 -steps 150 -nodes 4 -groups 8 -faults 0 -seed 1)" >> "$OUT"
echo "$($BIN -store sim -schedules 40 -steps 150 -nodes 4 -groups 8 -faults 15 -seed 100)" >> "$OUT"
echo "$($BIN -store sim -schedules 40 -steps 150 -nodes 4 -groups 8 -faults 40 -seed 200)" >> "$OUT"
echo "$($BIN -store sim -schedules 20 -steps 300 -nodes 8 -groups 16 -faults 15 -seed 300)" >> "$OUT"
echo "$($BIN -store sim -schedules 20 -steps 300 -nodes 2 -groups 2 -faults 40 -seed 400)" >> "$OUT"

if [ -n "${AKI_OBS1_S3:-}" ]; then
    bucket="obs1-fencetorture-$(date +%s)"
    curl -sf -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
        --user "${AKI_OBS1_S3_USER:-minioadmin}:${AKI_OBS1_S3_PASS:-minioadmin}" \
        "$AKI_OBS1_S3/$bucket" > /dev/null
    echo "$($BIN -store minio -bucket "$bucket" -schedules 5 -steps 150 -nodes 4 -groups 8 -seed 500)" >> "$OUT"
    echo "$($BIN -store minio -bucket "$bucket" -schedules 3 -steps 200 -nodes 8 -groups 16 -seed 600)" >> "$OUT"
else
    echo "AKI_OBS1_S3 unset, skipping the minio arms" >&2
fi

column -s, -t "$OUT"
