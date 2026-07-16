#!/bin/bash
# S0 exit gate: baseline table with f3 riding the aki slot, same matrix
# and knobs as the selfproof1 manifest. Split into six sub-runs (arm x
# mix) with sibling OUTDIRs so a WSL teardown costs at most one sub-run;
# run.sh truncates its results.csv at start, so the merge happens off-box.
set -u
cd /root/sqlo1bench/aki-bench/scenarios/sqlo1-core
export CAP_MIB=128 DURATION=10s WARM=5s REP_TIMEOUT=240
export REDIS=/root/bin/redis-server VALKEY=/root/bin/valkey-server
export AKI_DIR=/root/sqlo1bench/aki WORKDIR=/root
export PORT_AKI=7331 PORT_REDIS=7332 PORT_VALKEY=7333
export AKISLOT=f3
export SIZES='16 128 512 4096' DISTS='uniform zipfian' SCALES='1 4 16'

for arm in cap data; do
  for mix in 90 50 10; do
    OUTDIR=/root/sqlo1bench/f3base1-$arm-r$mix ARMS=$arm MIXES=$mix bash run.sh \
      || echo "SUBRUN $arm-r$mix nonzero: $?"
  done
done

echo 'F3BASE DONE'
ls /root/sqlo1bench/f3base1-*/ | grep -c json || true
