package main

import "testing"

func TestMeanSpread(t *testing.T) {
	mean, spread := meanSpread([]float64{90, 100, 110})
	if mean != 100 {
		t.Fatalf("mean = %v, want 100", mean)
	}
	if spread != 0.2 {
		t.Fatalf("spread = %v, want 0.2", spread)
	}
	if m, s := meanSpread(nil); m != 0 || s != 0 {
		t.Fatalf("empty = %v %v, want zeros", m, s)
	}
}

func TestKeysCycleTheKeyspace(t *testing.T) {
	if key("k", 0) != "k:0" || key("k", keyspace) != "k:0" || key("k", keyspace+7) != "k:7" {
		t.Fatalf("key cycling broken: %s %s %s", key("k", 0), key("k", keyspace), key("k", keyspace+7))
	}
	for _, fam := range families {
		args := fam.op(12345)
		if len(args) == 0 {
			t.Fatalf("family %s produced an empty command", fam.name)
		}
	}
}
