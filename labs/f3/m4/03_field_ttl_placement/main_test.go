package main

import "testing"

// The lab's claims as invariants, so CI catches a placement regression: no TTL
// costs zero expiry bytes on both bands, any TTL costs exactly eight bytes a
// field, and half a hash TTL'd costs the same as all of it (whole-container).

func TestNativeExpColumnPlacement(t *testing.T) {
	for _, m := range []int{8, 64, 512, 4096} {
		_, _, _, none := nativeBand(m, 64, 0)
		if none != nil {
			t.Fatalf("m=%d native exp column allocated with no TTL: len %d", m, len(none))
		}
		_, _, _, all := nativeBand(m, 64, m)
		if len(all) != m {
			t.Fatalf("m=%d native exp column len %d, want %d", m, len(all), m)
		}
		// Half a hash TTL'd still allocates the whole column: eight bytes a field.
		_, _, _, half := nativeBand(m, 64, m/2)
		if len(half) != m {
			t.Fatalf("m=%d half-TTL native column len %d, want %d (whole container)", m, len(half), m)
		}
	}
}

func TestInlineStickyPlacement(t *testing.T) {
	for _, m := range []int{8, 64, 512} {
		for _, w := range []int{8, 64} {
			no := len(inlineBlob(m, w, 0))
			all := len(inlineBlob(m, w, m))
			if got := all - no; got != inlineExpSize*m {
				t.Fatalf("m=%d w=%d inline TTL delta %d, want %d (8/field)", m, w, got, inlineExpSize*m)
			}
			// Any TTL flips the whole blob sticky, so half costs the same as all.
			if half := len(inlineBlob(m, w, m/2)); half != all {
				t.Fatalf("m=%d w=%d half-TTL blob %d != all-TTL %d (whole container)", m, w, half, all)
			}
		}
	}
}

// inlineExpSize is the listpackex per-entry slot, mirrored from hash.go.
const inlineExpSize = 8
