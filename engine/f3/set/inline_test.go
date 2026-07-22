package set

import (
	"slices"
	"testing"
)

// A packed inline set reloads into a scratch set that answers card, membership,
// and iteration identically, for both the intset and the listpack band.
func TestInlineRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		want encoding
		ms   []string
	}{
		{"intset", encIntset, []string{"3", "1", "2", "-9", "1000000"}},
		{"listpack", encListpack, []string{"alpha", "beta", "gamma", "beta"}},
		{"listpack-with-ints", encListpack, []string{"7", "hello", "9"}},
		{"single-int", encIntset, []string{"42"}},
		{"single-str", encListpack, []string{"x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := buildSet(tc.ms)
			if src.enc != tc.want {
				t.Fatalf("built encoding = %v, want %v", src.enc, tc.want)
			}
			if !inlineEligible(src) {
				t.Fatal("built set not inline-eligible")
			}

			var dst set
			loadInline(&dst, src.data, inlineBits(src), 12345)

			if dst.enc != src.enc {
				t.Fatalf("reloaded enc = %v, want %v", dst.enc, src.enc)
			}
			if dst.expireAt != 12345 {
				t.Fatalf("reloaded expireAt = %d, want 12345", dst.expireAt)
			}
			if dst.card() != src.card() {
				t.Fatalf("reloaded card = %d, want %d", dst.card(), src.card())
			}
			if got, want := members(&dst), members(src); !slices.Equal(got, want) {
				t.Fatalf("reloaded members = %v, want %v", got, want)
			}
			// Membership answers the same for a present and an absent member.
			for _, m := range tc.ms {
				if !dst.has([]byte(m)) {
					t.Fatalf("reloaded set missing member %q", m)
				}
			}
			if dst.has([]byte("definitely-absent-0xZZ")) {
				t.Fatal("reloaded set reports an absent member present")
			}
		})
	}
}

// The scratch set's data capacity is reused across reloads (no fresh slice per
// load), and a mutation on the reloaded set does not scribble the source blob:
// loadInline copies the blob in.
func TestInlineLoadCopiesAndReuses(t *testing.T) {
	src := buildSet([]string{"alpha", "beta"})
	blob := append([]byte(nil), src.data...)

	var dst set
	loadInline(&dst, blob, inlineBits(src), 0)
	// Grow the reloaded set; the source blob must be untouched.
	dst.add([]byte("gamma"))
	if string(blob) != string(src.data) {
		t.Fatal("mutating the reloaded set scribbled the source blob")
	}

	// A second load reuses dst.data's backing (cap does not shrink), the
	// per-shard-scratch reuse the routing slice relies on.
	capBefore := cap(dst.data)
	loadInline(&dst, blob, inlineBits(src), 0)
	if cap(dst.data) < capBefore {
		t.Fatalf("reload shrank data cap %d -> %d", capBefore, cap(dst.data))
	}
	if dst.card() != 2 {
		t.Fatalf("reloaded card = %d, want 2", dst.card())
	}
}

// encFromBits masks off the high bits the routing slice rides the idle clock
// in, so the encoding survives a bits word carrying a clock.
func TestEncFromBitsMasksClock(t *testing.T) {
	for _, enc := range []encoding{encIntset, encListpack} {
		bits := uint16(enc) & inlineEncMask
		// Simulate the routing slice's idle clock riding the high fifteen bits.
		bits |= 0x5ABC << 1
		if got := encFromBits(bits); got != enc {
			t.Fatalf("encFromBits(%#x) = %v, want %v", bits, got, enc)
		}
	}
}

// countListpackEntries matches set.n over a listpack blob built by the real
// append path, across sizes.
func TestCountListpackEntries(t *testing.T) {
	for _, size := range []int{0, 1, 5, maxListpackEntries} {
		s := &set{enc: encListpack}
		for i := 0; i < size; i++ {
			s.add([]byte("m" + itoa(int64(i))))
		}
		if got := countListpackEntries(s.data); got != s.n {
			t.Fatalf("size %d: countListpackEntries = %d, want n = %d", size, got, s.n)
		}
	}
}

// An escalated set (native table, past the listpack cap) is not inline
// eligible: it is no longer a single packed blob.
func TestInlineEligibleExcludesEscalated(t *testing.T) {
	s := &set{enc: encListpack}
	for i := 0; i <= maxListpackEntries; i++ {
		s.add([]byte("member-" + itoa(int64(i))))
	}
	if s.enc != encHashtable {
		t.Fatalf("set did not escalate: enc = %v", s.enc)
	}
	if inlineEligible(s) {
		t.Fatal("escalated set reported inline-eligible")
	}
}
