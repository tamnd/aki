package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// The packed-pair codec (spec 2064/obs1 doc 08 section 3). These tests pin the
// three properties the fieldttl lab (#1294) picked this encoding for: a chunk
// with no TTL bearer packs byte-identical to a codec with no TTL support at
// all, a bearer chunk round-trips every expiry through the presence bitmap,
// and a torn payload reports as torn instead of decoding garbage.

// plainPack hand-encodes the no-TTL form: uvarint flen, field, uvarint vlen,
// value. This is the byte-identity baseline Finish must match when no entry
// carries an expiry.
func plainPack(pairs [][2]string) []byte {
	var b []byte
	for _, p := range pairs {
		b = binary.AppendUvarint(b, uint64(len(p[0])))
		b = append(b, p[0]...)
		b = binary.AppendUvarint(b, uint64(len(p[1])))
		b = append(b, p[1]...)
	}
	return b
}

func TestChunkPackerNoBearerByteIdentity(t *testing.T) {
	pairs := [][2]string{{"alpha", "1"}, {"", "empty field"}, {"beta", ""}, {"g", "vvvvvvvvvvvvvvvv"}}
	var pk ChunkPacker
	for _, p := range pairs {
		pk.Add([]byte(p[0]), []byte(p[1]), 0)
	}
	payload, flags := pk.Finish()
	if flags != 0 {
		t.Fatalf("flags 0x%02x on a bearer-free chunk, want 0", flags)
	}
	if want := plainPack(pairs); !bytes.Equal(payload, want) {
		t.Fatalf("payload %x not byte-identical to the plain form %x", payload, want)
	}
	if pk.Bytes() != len(payload) {
		t.Fatalf("Bytes %d disagrees with the bearer-free payload length %d", pk.Bytes(), len(payload))
	}
}

func TestChunkPackerBearerRoundTrip(t *testing.T) {
	// 20 entries so the bitmap spans multiple bytes; expiries on a scattered
	// subset so both bit states appear in every bitmap byte.
	type ent struct {
		field, value string
		exp          uint64
	}
	var ents []ent
	var pk ChunkPacker
	for i := 0; i < 20; i++ {
		e := ent{field: fmt.Sprintf("f%02d", i), value: fmt.Sprintf("v%02d", i)}
		if i%3 == 0 {
			e.exp = uint64(1700000000000 + i)
		}
		ents = append(ents, e)
		pk.Add([]byte(e.field), []byte(e.value), e.exp)
	}
	payload, flags := pk.Finish()
	if flags != ChunkFlagTTLBitmap {
		t.Fatalf("flags 0x%02x on a bearer chunk, want ChunkFlagTTLBitmap", flags)
	}
	if pk.Count() != len(ents) {
		t.Fatalf("count %d want %d", pk.Count(), len(ents))
	}
	seen := 0
	ok := WalkPackedPairs(payload, flags, pk.Count(), func(i int, p PackedPair) bool {
		e := ents[i]
		if string(p.Field) != e.field || string(p.Value) != e.value || p.Exp != e.exp {
			t.Fatalf("entry %d = (%q, %q, %d), want (%q, %q, %d)", i, p.Field, p.Value, p.Exp, e.field, e.value, e.exp)
		}
		seen++
		return true
	})
	if !ok || seen != len(ents) {
		t.Fatalf("walk ok=%v saw %d of %d", ok, seen, len(ents))
	}
	// The point read agrees with the walk at every index.
	for i, e := range ents {
		p, ok := PackedPairAt(payload, flags, len(ents), i)
		if !ok || string(p.Field) != e.field || string(p.Value) != e.value || p.Exp != e.exp {
			t.Fatalf("PackedPairAt(%d) = (%q, %q, %d, %v), want (%q, %q, %d)", i, p.Field, p.Value, p.Exp, ok, e.field, e.value, e.exp)
		}
	}
}

func TestChunkPackerResetReuse(t *testing.T) {
	var pk ChunkPacker
	pk.Add([]byte("a"), []byte("1"), 77)
	pk.Finish()
	pk.Reset()
	if pk.Count() != 0 || pk.Bytes() != 0 {
		t.Fatalf("count %d bytes %d after Reset", pk.Count(), pk.Bytes())
	}
	pk.Add([]byte("b"), []byte("2"), 0)
	payload, flags := pk.Finish()
	if flags != 0 {
		t.Fatalf("flags 0x%02x after Reset: the old chunk's bearer leaked", flags)
	}
	p, ok := PackedPairAt(payload, flags, 1, 0)
	if !ok || string(p.Field) != "b" || string(p.Value) != "2" || p.Exp != 0 {
		t.Fatalf("post-Reset entry = (%q, %q, %d, %v)", p.Field, p.Value, p.Exp, ok)
	}
}

func TestWalkPackedPairsTorn(t *testing.T) {
	var pk ChunkPacker
	pk.Add([]byte("field"), []byte("value"), 1700000000000)
	pk.Add([]byte("plain"), []byte("v"), 0)
	full, flags := pk.Finish()
	nop := func(int, PackedPair) bool { return true }

	// Every truncation point is torn: bitmap, varints, field bytes, the
	// expiry word, value bytes.
	for cut := 0; cut < len(full); cut++ {
		if WalkPackedPairs(full[:cut], flags, 2, nop) {
			t.Fatalf("payload truncated to %d of %d bytes decoded", cut, len(full))
		}
	}
	// A count past the payload is torn, not a short read.
	if WalkPackedPairs(full, flags, 3, nop) {
		t.Fatal("a count past the payload decoded")
	}
	// A plain payload read under the bitmap flag misparses or tears; it must
	// not panic, and a bitmap payload read as plain must still frame-decode
	// without running past the buffer (the expiry bytes just misread as an
	// entry). Both directions are exercised for bounds safety only.
	WalkPackedPairs(full, 0, 2, nop)
	plain := plainPack([][2]string{{"f", "v"}})
	WalkPackedPairs(plain, ChunkFlagTTLBitmap, 1, nop)
}

func TestPackedPairAtBounds(t *testing.T) {
	var pk ChunkPacker
	pk.Add([]byte("only"), []byte("one"), 0)
	payload, flags := pk.Finish()
	if _, ok := PackedPairAt(payload, flags, 1, -1); ok {
		t.Fatal("a negative index decoded")
	}
	if _, ok := PackedPairAt(payload, flags, 1, 1); ok {
		t.Fatal("an index at count decoded")
	}
	if _, ok := PackedPairAt(nil, flags, 0, 0); ok {
		t.Fatal("an empty payload decoded an entry")
	}
}

// TestWalkPackedPairsEarlyStop pins the early-stop contract: fn returning
// false ends the walk with a true (well-formed) verdict even when later
// entries were never visited.
func TestWalkPackedPairsEarlyStop(t *testing.T) {
	var pk ChunkPacker
	pk.Add([]byte("a"), []byte("1"), 0)
	pk.Add([]byte("b"), []byte("2"), 0)
	payload, flags := pk.Finish()
	visits := 0
	ok := WalkPackedPairs(payload, flags, 2, func(i int, p PackedPair) bool {
		visits++
		return false
	})
	if !ok || visits != 1 {
		t.Fatalf("early stop: ok=%v visits=%d, want true and 1", ok, visits)
	}
}
