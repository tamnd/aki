#!/usr/bin/env bash
# Interleaved A/B for the OBJECT IDLETIME access-clock stamp on the hot GET path.
#
# BenchmarkGetView drives the GET command path (store.GetView), the one that now
# writes the per-key access clock into the record header's spare offKindBits word
# on every hit. This script measures that write's cost by toggling the stamp line
# in view.go off and on and running the benchmark on each side, INTERLEAVED, so a
# thermal or background-load drift that walks between runs cannot be read as a
# regression (an early non-interleaved pass showed a spurious 3x that vanished
# under interleaving; the laptop, not the store, had moved).
#
# Run from the aki repo root: bash labs/f3/m9/01_idle_clock_stamp/run.sh
set -euo pipefail

VIEW=engine/f3/store/view.go
WITH=$'\ts.touchSlot(slot)\n\ts.stampClock(addr, now)\n\treturn s.readValueRef(addr)'
WITHOUT=$'\ts.touchSlot(slot)\n\treturn s.readValueRef(addr)'

bench() {
  go test ./engine/f3/store/ -run '^$' -bench 'BenchmarkGetView$' -benchtime 2s -count 5 2>&1 \
    | awk '/BenchmarkGetView/{print $3}' | sort -n \
    | awk '{a[NR]=$1} END{printf "median %s  (min %s  max %s)\n", a[int(NR/2)+1], a[1], a[NR]}'
}

off() { perl -0pi -e "s/\Q$WITH\E/$WITHOUT/" "$VIEW"; }
on()  { perl -0pi -e "s/\Q$WITHOUT\E/$WITH/" "$VIEW"; }

trap 'on 2>/dev/null || true' EXIT   # always restore the stamp
for r in 1 2 3; do
  printf "round %s WITH stamp:    " "$r"; bench
  off
  printf "round %s WITHOUT stamp: " "$r"; bench
  on
done
echo "stampClock occurrences in $VIEW (want 2): $(grep -c stampClock "$VIEW")"
