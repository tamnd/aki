package sqlo1b

import "testing"

// The batching decision is pure, so its branches test on any
// platform; the Linux integration tests cover the enters the plan
// actually produces.
func TestRingFlushPlan(t *testing.T) {
	const sq, cq = 32, 64
	cases := []struct {
		name       string
		pending    int
		inflight   int
		queueEmpty bool
		want       bool
	}{
		{"nothing pending", 0, 0, true, false},
		{"drain tick fires on any pending", 1, 0, true, true},
		{"full sq must flush", sq, 0, false, true},
		{"under target accumulates", ringBatchTarget - 1, 0, false, false},
		{"target fires", ringBatchTarget, 0, false, true},
		{"pressure drops the target", ringBatchLow, cq / 2, false, true},
		{"pressure boundary is half the cq", ringBatchLow, cq/2 - 1, false, false},
		{"under the low target holds even pressured", ringBatchLow - 1, cq, false, false},
	}
	for _, c := range cases {
		if got := ringFlushNow(c.pending, sq, c.inflight, cq, c.queueEmpty); got != c.want {
			t.Errorf("%s: ringFlushNow(%d, %d, %d, %d, %v) = %v, want %v",
				c.name, c.pending, sq, c.inflight, cq, c.queueEmpty, got, c.want)
		}
	}
}
