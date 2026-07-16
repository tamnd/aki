#!/usr/bin/env bash
# The PRED-OBS1-O0A-POOL scoring sweep: the pool's ceiling first, then the
# open-loop rate ladder through and past it, all against the simulator's
# doc 01 latency model. Everything is in-process; nothing to start.
set -euo pipefail
cd "$(dirname "$0")"

out="${1:-.}/poolrate.csv"
go build -o /tmp/obs1-poolrate .

/tmp/obs1-poolrate -header > "$out"

# The ceiling the pool supports, Little's law made empirical, at the
# candidate pool and one size up for context.
for pool in 128 256 512; do
	/tmp/obs1-poolrate -arm closed -pool $pool >> "$out"
done

# The rate ladder at pool 256: comfortably under, the 5,000 claim itself,
# approaching the ceiling, and past it so the queuing signature the
# prediction rules out is on record for contrast.
for rate in 2500 5000 6500 8000 9500; do
	/tmp/obs1-poolrate -arm open -rate $rate -pool 256 >> "$out"
done

# The same claim at pool 128, where 5,000 sits past the ceiling and must
# queue: the prediction's failure mode, shown to be reachable by the
# harness so a zero slot wait at 256 means something.
for rate in 2500 5000; do
	/tmp/obs1-poolrate -arm open -rate $rate -pool 128 >> "$out"
done

echo "wrote $out"
