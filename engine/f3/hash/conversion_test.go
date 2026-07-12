package hash

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// The conversion differential, this slice's headline (spec 2064/f3/10 section
// 4.2): the same logical hash built to stay inline and built to be forced native
// must answer every point command identically, so a client can never tell which
// side of the band boundary it is on. The property is checked over randomized
// field sets and randomized probe sets that mix present and absent fields, and
// the answers of HGET, HMGET, HEXISTS, HSTRLEN, and HLEN are compared field by
// field across the boundary.

// buildInline seats pairs on a hash left in the inline band.
func buildInline(pairs [][2]string) *hash {
	h := newHash()
	for _, p := range pairs {
		h.set([]byte(p[0]), []byte(p[1]))
	}
	return h
}

// buildNative seats the same pairs on a hash forced into the native band up
// front, so its logical content matches buildInline but its representation is the
// field table.
func buildNative(pairs [][2]string) *hash {
	h := forceNative(newHash())
	for _, p := range pairs {
		h.set([]byte(p[0]), []byte(p[1]))
	}
	return h
}

func TestConversionDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x1234, 0x5678))
	for round := 0; round < 200; round++ {
		pairs := randomPairs(rng)
		inline := buildInline(pairs)
		native := buildNative(pairs)

		// Both builds stay under the inline caps, so the inline side must not have
		// promoted; the native side is forced. That is the whole point of the test:
		// identical content, opposite bands.
		if inline.enc != encListpack {
			t.Fatalf("round %d: inline build unexpectedly promoted to %s", round, inline.enc)
		}
		if native.enc != encHashtable {
			t.Fatalf("round %d: native build is %s, want hashtable", round, native.enc)
		}

		if inline.card() != native.card() {
			t.Fatalf("round %d: HLEN inline %d, native %d", round, inline.card(), native.card())
		}

		// Probe present fields, absent fields, and repeats, the HMGET shape.
		for _, f := range probeFields(rng, pairs) {
			fb := []byte(f)
			vi, oi := inline.get(fb)
			vn, on := native.get(fb)
			if oi != on || string(vi) != string(vn) {
				t.Fatalf("round %d: HGET(%q) inline (%q,%v), native (%q,%v)", round, f, vi, oi, vn, on)
			}
			if inline.has(fb) != native.has(fb) {
				t.Fatalf("round %d: HEXISTS(%q) inline %v, native %v", round, f, inline.has(fb), native.has(fb))
			}
			if inline.strlen(fb) != native.strlen(fb) {
				t.Fatalf("round %d: HSTRLEN(%q) inline %d, native %d", round, f, inline.strlen(fb), native.strlen(fb))
			}
		}
	}
}

// randomPairs builds a small hash that stays within the inline caps: a handful of
// fields with short names and short values, some field names repeated so a later
// write overwrites rather than adds.
func randomPairs(rng *rand.Rand) [][2]string {
	n := 1 + rng.IntN(20)
	out := make([][2]string, 0, n)
	for i := 0; i < n; i++ {
		f := "f" + strconv.Itoa(rng.IntN(12)) // small field space forces overwrites
		v := "v" + strconv.Itoa(rng.IntN(1000))
		out = append(out, [2]string{f, v})
	}
	return out
}

// probeFields returns a field list mixing every field that was written (present)
// with a batch of names that were not (absent), so the differential exercises
// both hits and misses.
func probeFields(rng *rand.Rand, pairs [][2]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range pairs {
		if !seen[p[0]] {
			seen[p[0]] = true
			out = append(out, p[0])
		}
	}
	for i := 0; i < 5; i++ {
		out = append(out, "absent"+strconv.Itoa(rng.IntN(100)))
	}
	// An empty-string field name is a legal miss on both bands.
	out = append(out, "")
	return out
}
