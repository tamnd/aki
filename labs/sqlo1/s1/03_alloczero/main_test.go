package main

import "testing"

// TestWirePathZeroAlloc is the CI regression gate: every gated
// operation must run without a single steady-state allocation. A new
// hot path joins the gate by joining wireProbes (or a sibling probe
// list when the hot tier lands).
func TestWirePathZeroAlloc(t *testing.T) {
	for _, p := range wireProbes() {
		if allocs := testing.AllocsPerRun(1000, p.f); allocs != 0 {
			t.Errorf("%s: %.1f allocs/op, want 0", p.name, allocs)
		}
	}
}
