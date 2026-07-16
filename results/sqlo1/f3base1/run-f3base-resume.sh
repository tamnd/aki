#!/bin/bash
# Resume of run-f3base.sh after the WSL session tree died at cap-r10
# row 87. Sizes 16 and 128 of cap-r10 are complete in f3base1-cap-r10;
# this reruns sizes 512 and 4096 into a sibling dir (merge off-box,
# dropping the old partial 512 rows), then runs the three data-arm
# sub-runs the original never reached.
set -u
cd /root/sqlo1bench/aki-bench/scenarios/sqlo1-core
export CAP_MIB=128 DURATION=10s WARM=5s REP_TIMEOUT=240
export REDIS=/root/bin/redis-server VALKEY=/root/bin/valkey-server
export AKI_DIR=/root/sqlo1bench/aki WORKDIR=/root
export PORT_AKI=7331 PORT_REDIS=7332 PORT_VALKEY=7333
export AKISLOT=f3
export DISTS='uniform zipfian' SCALES='1 4 16'

OUTDIR=/root/sqlo1bench/f3base1-cap-r10b ARMS=cap MIXES=10 SIZES='512 4096' bash run.sh \
  || echo "SUBRUN cap-r10b nonzero: $?"
for mix in 90 50 10; do
  OUTDIR=/root/sqlo1bench/f3base1-data-r$mix ARMS=data MIXES=$mix SIZES='16 128 512 4096' bash run.sh \
    || echo "SUBRUN data-r$mix nonzero: $?"
done
echo 'F3BASE-RESUME DONE'
