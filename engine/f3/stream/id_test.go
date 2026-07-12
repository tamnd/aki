package stream

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestIDCompareOrdersMsThenSeq(t *testing.T) {
	cases := []struct {
		a, b streamID
		want int
	}{
		{streamID{1, 0}, streamID{1, 0}, 0},
		{streamID{1, 0}, streamID{1, 1}, -1},
		{streamID{1, 5}, streamID{1, 4}, 1},
		{streamID{1, 9}, streamID{2, 0}, -1}, // ms dominates seq
		{streamID{2, 0}, streamID{1, 9}, 1},
	}
	for _, c := range cases {
		if got := c.a.cmp(c.b); got != c.want {
			t.Fatalf("cmp(%s,%s)=%d want %d", c.a, c.b, got, c.want)
		}
		if got := c.a.less(c.b); got != (c.want < 0) {
			t.Fatalf("less(%s,%s)=%v want %v", c.a, c.b, got, c.want < 0)
		}
	}
}

func TestIDStringMatchesRedisForm(t *testing.T) {
	if got := (streamID{1526919030474, 55}).String(); got != "1526919030474-55" {
		t.Fatalf("String()=%q want ms-seq form", got)
	}
}

func TestIDDeltaRoundTrip(t *testing.T) {
	base := streamID{1000, 3}
	ids := []streamID{
		{1000, 3},         // the base itself: zero delta
		{1000, 9},         // same ms, seq forward
		{1001, 0},         // ms rolled, seq reset below base: negative seq delta
		{1005, 2},         // both forward
		{2000, 0},         // far ms, seq below base
		{1000, 3 + 1<<20}, // large positive seq delta
		{1000 + 1<<30, 3}, // large ms delta
	}
	for _, id := range ids {
		buf := putIDDelta(nil, base, id)
		if len(buf) != idDeltaLen(base, id) {
			t.Fatalf("id %s: encoded %d bytes, idDeltaLen said %d", id, len(buf), idDeltaLen(base, id))
		}
		got, n := readIDDelta(buf, base)
		if n != len(buf) {
			t.Fatalf("id %s: readIDDelta consumed %d of %d", id, n, len(buf))
		}
		if got != id {
			t.Fatalf("id %s: round-tripped to %s", id, got)
		}
	}
}

func TestSeqDeltaStaysSmallAcrossMillisecondBoundary(t *testing.T) {
	// The block firstID sits high in a millisecond, then the millisecond rolls
	// and seq restarts at 0: the delta is negative and must stay a 1-2 byte
	// signed varint, not the 10-byte uint64 underflow a plain uvarint produces.
	base := streamID{1000, 900}
	across := putIDDelta(nil, base, streamID{1001, 0}) // seq delta -900
	if len(across) > 3 {
		t.Fatalf("cross-boundary delta is %d bytes, want <=3 (ms 1 byte + seq 2 bytes)", len(across))
	}
	// The same-magnitude positive delta encodes to the same seq width, proving
	// the sign is not what blows up the size.
	forward := putIDDelta(nil, base, streamID{1001, 1800})
	if len(across) != len(forward) {
		t.Fatalf("negative seq delta %d bytes vs positive %d bytes, sign changed the width", len(across), len(forward))
	}
}

func TestVarintLenHelpersMatchStdlib(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var buf [binary.MaxVarintLen64]byte
	for i := 0; i < 5000; i++ {
		u := rng.Uint64() >> (rng.Intn(64))
		if got, want := uvlen(u), binary.PutUvarint(buf[:], u); got != want {
			t.Fatalf("uvlen(%d)=%d want %d", u, got, want)
		}
		s := int64(u)
		if rng.Intn(2) == 0 {
			s = -s
		}
		if got, want := vlen(s), binary.PutVarint(buf[:], s); got != want {
			t.Fatalf("vlen(%d)=%d want %d", s, got, want)
		}
	}
}
