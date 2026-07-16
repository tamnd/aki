#!/usr/bin/env bash
# The PRED-OBS1-O0B-APPEND scoring sweep: backoff policies head to head at
# the worst contention, then the contender ladder and the rate ladder on
# the policy the append loop will bake. The sim arms need nothing running;
# the minio arms run only when AKI_OBS1_S3 is set (start one first:
#   MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
#   ~/bin/minio server /tmp/obs1-minio-data --address 127.0.0.1:19000).
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/chainappend.csv"
go build -o /tmp/obs1-chainappend .

/tmp/obs1-chainappend -header > "$out"

# Policy sweep at the doc 02 worst case: 16 nodes, design rate.
for policy in none spec fixed; do
	/tmp/obs1-chainappend -policy $policy -contenders 16 -rate 20 >> "$out"
done

# Contender ladder on the spec policy, design rate per node.
for c in 1 2 4 8; do
	/tmp/obs1-chainappend -policy spec -contenders $c -rate 20 >> "$out"
done

# Rate ladder at 16 nodes: 4x and 16x the design point, where coalescing
# has to carry the load because the chain itself cannot go faster.
for rate in 80 320; do
	/tmp/obs1-chainappend -policy spec -contenders 16 -rate $rate >> "$out"
done

# Live cross-check on MinIO, fresh bucket per invocation so reruns never
# collide with an existing chain.
if [ -n "${AKI_OBS1_S3:-}" ]; then
	bucket="obs1-chainappend-$(date +%s)"
	curl -sf -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
		--user "${AKI_OBS1_S3_USER:-minioadmin}:${AKI_OBS1_S3_PASS:-minioadmin}" \
		"$AKI_OBS1_S3/$bucket" > /dev/null
	for c in 1 4 16; do
		/tmp/obs1-chainappend -store minio -bucket "$bucket" -policy spec -contenders $c -rate 20 >> "$out"
	done
	/tmp/obs1-chainappend -store minio -bucket "$bucket" -policy none -contenders 16 -rate 20 >> "$out"
else
	echo "AKI_OBS1_S3 unset, skipping the minio arms" >&2
fi

echo "wrote $out"
