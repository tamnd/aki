#!/usr/bin/env bash
# Interleaved A/B for the collection OBJECT IDLETIME access-clock stamp on the hot
# collection funnel.
#
# BenchmarkSetLive drives set.(*reg).live, the one funnel every set read and write
# routes through, which now stamps the per-key access clock into the set struct's
# spare padding word on every hit. This script prices that stamp by commenting the
# stamp line in set/reg.go out and back in and running the benchmark on each side,
# INTERLEAVED, so a thermal or background-load drift that walks between runs cannot
# be read as a regression (see the m9/01 string lab, where a non-interleaved pass
# showed a spurious 3x that vanished under interleaving).
#
# The live funnel is representative of all five collection types: every type took
# the same live/peek split and the same one-line store.LRUClock stamp, so the set
# number stands in for zset, hash, list, and stream too.
#
# Run from the aki repo root: bash labs/f3/m9/02_collection_idle_clock/run.sh
set -euo pipefail

REG=engine/f3/set/reg.go
STAMP='s.clock = store.LRUClock(cx.NowMs)'

bench() {
  go test ./engine/f3/set/ -run '^$' -bench 'BenchmarkSetLive$' -benchtime 2s -count 5 2>&1 \
    | awk '/BenchmarkSetLive/{print $3}' | sort -n \
    | awk '{a[NR]=$1} END{printf "median %s  (min %s  max %s)\n", a[int(NR/2)+1], a[1], a[NR]}'
}

off() { sed -i '' "s|	$STAMP|	// $STAMP|" "$REG"; }   # prepend // (tab before $STAMP)
on()  { sed -i '' "s|	// $STAMP|	$STAMP|" "$REG"; }     # strip the //

trap 'on 2>/dev/null || true' EXIT   # always restore the stamp
for r in 1 2 3; do
  printf "round %s WITH stamp:    " "$r"; bench
  off
  printf "round %s WITHOUT stamp: " "$r"; bench
  on
done
echo "active (uncommented) stamp lines in $REG (want 1): $(grep -c '^		s.clock = store.LRUClock' "$REG")"
