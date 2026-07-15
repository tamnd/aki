package main

import "testing"

// The tests pin the sweep's qualitative verdicts in quick configuration,
// so CI catches a model change that would silently invalidate a baked
// constant. Margins are wide of the measured gaps; the exact numbers live
// in the README.

func quickTraces(t *testing.T) []traceConfig {
	t.Helper()
	return traces(true)
}

func TestSampledPromotionBeatsUnconditional(t *testing.T) {
	capacity := capacityFor(true)
	for _, tc := range quickTraces(t) {
		low := run(tc, policyWatt2, capacity, 64, 0.125)
		full := run(tc, policyWatt2, capacity, 64, 1.0)
		if low < full+0.01 {
			t.Errorf("%s: D=0.125 hit %.4f, D=1.0 hit %.4f; sampled promotion must win by at least 1pp", tc.name, low, full)
		}
	}
}

func TestPromotionIsTheOnlyDoorOnReadOnly(t *testing.T) {
	capacity := capacityFor(true)
	for _, tc := range quickTraces(t) {
		if tc.writeFrac > 0 {
			continue
		}
		if hit := run(tc, policyWatt2, capacity, 64, 0); hit != 0 {
			t.Errorf("%s: D=0 hit %.4f, want 0; with no writes the coin is the only way into the tier", tc.name, hit)
		}
	}
}

func TestThirdTimestampBuysLittle(t *testing.T) {
	capacity := capacityFor(true)
	for _, tc := range quickTraces(t) {
		w2 := run(tc, policyWatt2, capacity, 64, 0.125)
		w3 := run(tc, policyWatt3, capacity, 64, 0.125)
		if w3-w2 > 0.015 {
			t.Errorf("%s: watt3 %.4f vs watt2 %.4f; a gain past 1.5pp would reopen the 8-bytes-per-record question", tc.name, w3, w2)
		}
	}
}

func TestSampleSizeIsFlat(t *testing.T) {
	capacity := capacityFor(true)
	for _, tc := range quickTraces(t) {
		small := run(tc, policyWatt2, capacity, 16, 0.125)
		big := run(tc, policyWatt2, capacity, 256, 0.125)
		diff := big - small
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.01 {
			t.Errorf("%s: K=16 hit %.4f vs K=256 hit %.4f; K was flat when the default was baked", tc.name, small, big)
		}
	}
}

func TestEvictorIsAWashAtVerdictD(t *testing.T) {
	capacity := capacityFor(true)
	for _, tc := range quickTraces(t) {
		clock := run(tc, policyClock, capacity, 64, 0.125)
		watt := run(tc, policyWatt2, capacity, 64, 0.125)
		diff := clock - watt
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.02 {
			t.Errorf("%s: clock %.4f vs watt2 %.4f; at D=0.125 the scoring policy was within noise when the constants were baked", tc.name, clock, watt)
		}
	}
}

func TestGhostRingRestoresHistory(t *testing.T) {
	// A key evicted into the ghost ring must re-enter the tier on its next
	// read even at D=0, with its stamps intact: the ghost ring is the
	// second chance the low promotion coin leans on.
	tr := newTier(policyWatt2, 32, 8, 1)
	for k := range int32(32) {
		tr.set(k)
	}
	tr.set(99) // forces an eviction into the ghost ring
	var ghosted int32 = -1
	for k := range tr.ghost {
		ghosted = k
		break
	}
	if ghosted < 0 {
		t.Fatal("eviction did not populate the ghost ring")
	}
	if hit := tr.get(ghosted, 0); hit {
		t.Fatal("a ghosted key must not report a hot hit")
	}
	if _, ok := tr.byKey[ghosted]; !ok {
		t.Fatal("a ghost hit must promote at D=0")
	}
	idx := tr.byKey[ghosted]
	if tr.slots[idx].write.n == 0 {
		t.Fatal("promotion from the ghost ring must restore the write stamps")
	}
}
