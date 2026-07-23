package main

import "testing"

func TestQueueEndSmoke(t *testing.T) {
	res, err := run(cfg{backlog: 60_000, rounds: 4, k: 50})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, p := range res.phases {
		if p.gets != 0 {
			t.Fatalf("phase %s took %d bucket GETs, want 0", p.name, p.gets)
		}
	}
	if res.coldBytes == 0 {
		t.Fatal("backlog interior never demoted")
	}
	if res.folds == 0 {
		t.Fatal("fold pipeline never ran")
	}
	if res.fetches != 0 || res.errs != 0 {
		t.Fatalf("cold reader fetches %d errs %d, want 0", res.fetches, res.errs)
	}
}
