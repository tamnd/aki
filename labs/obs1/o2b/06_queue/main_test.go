package main

import "testing"

// TestQueueSmoke runs the small configuration: harness mechanics only,
// the bands belong to the scored run.
func TestQueueSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("full server rig")
	}
	res, err := run(cfg{backlog: 60_000, rounds: 4, k: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range res.phases {
		if p.gets != 0 {
			t.Errorf("phase %s billed %d bucket GETs, want 0", p.name, p.gets)
		}
	}
	if res.queuePlaces == 0 {
		t.Error("queue key never reached the fold ledger")
	}
	if res.errs != 0 {
		t.Errorf("cold reader errs/unresolved = %d", res.errs)
	}
}
