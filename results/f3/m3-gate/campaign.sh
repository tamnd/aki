#!/bin/bash
cd /root/f3gate/m2m3
run_group() {
  local m=$1 g=$2
  for i in $(seq 1 25); do
    echo "[$(date -u +%H:%M:%S)] $m $g window $i START"
    python3 -u runner.py "$m" "$g" 2>&1 | tee /tmp/win_${m}_${g}
    grep -q "ALL CELLS DONE" /tmp/win_${m}_${g} && { echo "== $m $g COMPLETE =="; return 0; }
  done
  echo "== $m $g GAVE UP after 25 windows =="
}
echo "CAMPAIGN START $(date -u)"
run_group m2 main
run_group m2 alg
run_group m3 main
echo "CAMPAIGN COMPLETE $(date -u)"
