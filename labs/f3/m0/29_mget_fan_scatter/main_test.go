package main

import "testing"

// TestScatterEquivalence asserts the reused-scratch scatter hands every owner
// the identical slice of the command the make-per-command scatter did, over the
// MGET and MSET shapes and a spread of key counts, so the allocation elision is
// proven safe before its throughput is measured.
func TestScatterEquivalence(t *testing.T) {
	const shards = 8
	for _, keys := range []int{1, 2, 3, 8, 16, 64, 257} {
		for _, mget := range []bool{true, false} {
			k := synthKeys(keys)
			var vals [][]byte
			if !mget {
				vals = synthVals(keys, 64)
			}
			if err := verify(k, vals, shards, mget); err != nil {
				t.Fatalf("keys=%d mget=%v: %v", keys, mget, err)
			}
		}
	}
}

// TestScatterNewAllocFree asserts the reused-scratch scatter settles to zero
// allocations per command on the MGET gate cell once its scratch is warm, the
// property the elision buys.
func TestScatterNewAllocFree(t *testing.T) {
	const shards = 8
	keys := synthKeys(16)
	sc := &scatterer{}
	var dst sink
	// Warm the scratch so its buffers reach steady capacity.
	for i := 0; i < 4; i++ {
		dst.reset()
		sc.scatterNew(&dst, keys, nil, shards, perSubCap, true)
	}
	got := testing.AllocsPerRun(200, func() {
		dst.reset()
		sc.scatterNew(&dst, keys, nil, shards, perSubCap, true)
	})
	if got != 0 {
		t.Fatalf("warm MGET scatter allocated %.1f objects/op, want 0", got)
	}
}

func benchScatter(b *testing.B, keys int, mget, reuse bool) {
	k := synthKeys(keys)
	var vals [][]byte
	if !mget {
		vals = synthVals(keys, 64)
	}
	const shards = 8
	var dst sink
	sc := &scatterer{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.reset()
		if reuse {
			sc.scatterNew(&dst, k, vals, shards, perSubCap, mget)
		} else {
			scatterOld(&dst, k, vals, shards, perSubCap, mget)
		}
	}
}

func BenchmarkScatterOldMGET16(b *testing.B) { benchScatter(b, 16, true, false) }
func BenchmarkScatterNewMGET16(b *testing.B) { benchScatter(b, 16, true, true) }
func BenchmarkScatterOldMSET16(b *testing.B) { benchScatter(b, 16, false, false) }
func BenchmarkScatterNewMSET16(b *testing.B) { benchScatter(b, 16, false, true) }
