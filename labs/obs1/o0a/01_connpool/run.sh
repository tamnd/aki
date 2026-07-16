#!/usr/bin/env bash
# Sweep concurrency x pool limit across the arms into connpool.csv.
# The minio arm expects a MinIO at AKI_OBS1_S3 (default http://127.0.0.1:19000);
# start one first: MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
#   minio server /tmp/obs1-minio-data --address 127.0.0.1:19000
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/connpool.csv"
go build -o /tmp/obs1-connpool .

/tmp/obs1-connpool -header > "$out"

# Reuse mechanics: does the pool keep up as fan-out grows past it?
for conc in 1 4 16 64 128 256 512; do
	for pool in 32 64 128 256; do
		/tmp/obs1-connpool -arm inproc-http -conc $conc -pool $pool >> "$out"
	done
done

# TLS session cost, same shape, pool fixed to the candidate default.
for conc in 1 4 16 64 128 256 512; do
	/tmp/obs1-connpool -arm inproc-tls -conc $conc -pool 64 >> "$out"
done

# The per-connection setup floor: keep-alive off, plain and TLS both pay a
# dial per request (fresh is plain; inproc-tls already showed the handshake
# delta on reused connections).
for conc in 16 64 256; do
	/tmp/obs1-connpool -arm fresh -conc $conc -pool 64 >> "$out"
done

# Real-server cross-check on local MinIO, signed GETs.
for conc in 1 4 16 64 128 256 512; do
	for pool in 32 64 128 256; do
		/tmp/obs1-connpool -arm minio -conc $conc -pool $pool >> "$out"
	done
done

echo "wrote $out"
