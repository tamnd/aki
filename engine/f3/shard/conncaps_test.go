package shard

import "testing"

// resolveConnCaps derives repCap from batchDataCap by default and lets
// Config.RepCap override it outright, the labs/f3/m0/27_rep_headroom lever: a
// write-heavy load skips the batchDataCap+64*batchCap reply headroom because the
// buffer grows on demand. The other caps keep their tuning.go defaults when the
// override is zero.
func TestResolveConnCapsRepCap(t *testing.T) {
	t.Run("default derives from batchDataCap", func(t *testing.T) {
		var r Runtime
		r.resolveConnCaps(Config{})
		if want := batchDataCap + 64*batchCap; r.repCap != want {
			t.Fatalf("default repCap = %d, want %d", r.repCap, want)
		}
	})

	t.Run("tracks a swept batchDataCap", func(t *testing.T) {
		var r Runtime
		r.resolveConnCaps(Config{BatchDataCap: 1024})
		if want := 1024 + 64*batchCap; r.repCap != want {
			t.Fatalf("derived repCap = %d, want %d", r.repCap, want)
		}
	})

	t.Run("RepCap overrides the derived headroom", func(t *testing.T) {
		var r Runtime
		r.resolveConnCaps(Config{BatchDataCap: 1024, RepCap: 1024})
		if r.repCap != 1024 {
			t.Fatalf("overridden repCap = %d, want 1024", r.repCap)
		}
		if r.batchDataCap != 1024 {
			t.Fatalf("batchDataCap = %d, want 1024 (RepCap must not disturb it)", r.batchDataCap)
		}
	})

	t.Run("a fresh node starts at the overridden repCap", func(t *testing.T) {
		var r Runtime
		r.resolveConnCaps(Config{BatchDataCap: 1024, RepCap: 1024})
		b := newBatch(r.batchDataCap, r.repCap)
		if cap(b.rep) != 1024 {
			t.Fatalf("node rep cap = %d, want 1024", cap(b.rep))
		}
	})
}
