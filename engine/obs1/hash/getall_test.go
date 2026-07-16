package hash

import (
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The HGETALL/HKEYS/HVALS suite (spec 2064/f3/10 section 7.5). The stream half
// drains the enumeration source directly, the way set/partition_test.go drains
// SMEMBERS, so the framing and the pin are checked without depending on the
// command writer to pump a ring; the command half drives the buffered path
// through the harness for reply shape, the empty key, and WRONGTYPE.

// drainEnum drains an enumeration stream to completion through a deliberately
// small buffer, so the encoder has to straddle chunk boundaries mid-element, and
// returns the framed reply bytes.
func drainEnum(t *testing.T, src *enumStream, total int64) []byte {
	t.Helper()
	dst := make([]byte, 37) // not a frame multiple, forces mid-element splits
	var out []byte
	for int64(len(out)) < total {
		n, err := src.Next(dst)
		if err != nil {
			t.Fatalf("stream Next: %v", err)
		}
		if n == 0 {
			break
		}
		out = append(out, dst[:n]...)
	}
	if int64(len(out)) != total {
		t.Fatalf("stream produced %d bytes, enumTotal said %d", len(out), total)
	}
	return out
}

// TestEnumStreamPairs frames a native hash's HGETALL through the stream and checks
// it is exactly the hash: the field-value pairs, each field with its own value,
// every field once, and the byte width enumTotal promised.
func TestEnumStreamPairs(t *testing.T) {
	h := buildNative(pairsN(600))
	total := h.ft.enumTotal(enumPairs)
	src := h.ft.pinEnumStream(enumPairs)
	defer src.Release()

	arr := decodeReply(t, drainEnum(t, src, total)).([]any)
	if len(arr) != 1200 {
		t.Fatalf("HGETALL framed %d elements, want 1200 (600 pairs)", len(arr))
	}
	seen := map[string]bool{}
	for i := 0; i < len(arr); i += 2 {
		f := arr[i].(string)
		v := arr[i+1].(string)
		if seen[f] {
			t.Fatalf("field %q framed twice", f)
		}
		seen[f] = true
		if v != "v"+strconv.Itoa(fieldIndex([]byte(f))) {
			t.Fatalf("field %q paired with %q, want its own value", f, v)
		}
	}
	if len(seen) != 600 {
		t.Fatalf("HGETALL framed %d distinct fields, want 600", len(seen))
	}
}

// TestEnumStreamKeysVals checks the field-only and value-only projections frame
// the right column and the right count.
func TestEnumStreamKeysVals(t *testing.T) {
	h := buildNative(pairsN(400))
	for _, tc := range []struct {
		mode   enumMode
		prefix string
	}{
		{enumKeys, "f"},
		{enumVals, "v"},
	} {
		total := h.ft.enumTotal(tc.mode)
		src := h.ft.pinEnumStream(tc.mode)
		arr := decodeReply(t, drainEnum(t, src, total)).([]any)
		src.Release()
		if len(arr) != 400 {
			t.Fatalf("mode %d framed %d elements, want 400", tc.mode, len(arr))
		}
		seen := map[string]bool{}
		for _, e := range arr {
			s := e.(string)
			if s[0] != tc.prefix[0] {
				t.Fatalf("mode %d framed %q, want a %q-prefixed element", tc.mode, s, tc.prefix)
			}
			seen[s] = true
		}
		if len(seen) != 400 {
			t.Fatalf("mode %d framed %d distinct, want 400", tc.mode, len(seen))
		}
	}
}

// TestEnumStreamPinSurvivesChurn is the pin's proof: a stream opened over a
// snapshot keeps framing that snapshot even as the hash is deleted into and
// inserted onto underneath it. The pin freezes slab compaction and free-slot
// reuse, so a field removed after the snapshot is still returned (its bytes stay
// in the slab and its ordinal is not repurposed) and a field added after it is
// not.
func TestEnumStreamPinSurvivesChurn(t *testing.T) {
	h := buildNative(pairsN(600))
	total := h.ft.enumTotal(enumPairs)
	src := h.ft.pinEnumStream(enumPairs) // snapshot: f0..f599

	// Churn hard while the stream is pinned: drop half the fields and add fresh
	// ones. Deletes free ordinals and pile up dead bytes; inserts would normally
	// reuse those ordinals and could trigger compaction. The pin must block both.
	for i := 0; i < 300; i++ {
		h.del([]byte("f" + strconv.Itoa(i)))
		h.set([]byte("g"+strconv.Itoa(i)), []byte("w"+strconv.Itoa(i)))
	}

	arr := decodeReply(t, drainEnum(t, src, total)).([]any)
	src.Release()
	if len(arr) != 1200 {
		t.Fatalf("pinned stream framed %d elements, want the 1200 of the snapshot", len(arr))
	}
	for i := 0; i < len(arr); i += 2 {
		f := arr[i].(string)
		if f[0] != 'f' {
			t.Fatalf("pinned stream framed %q, a field added after the snapshot", f)
		}
		if arr[i+1].(string) != "v"+strconv.Itoa(fieldIndex([]byte(f))) {
			t.Fatalf("pinned field %q lost its value under churn", f)
		}
	}
	// The hash itself moved on: 600 - 300 + 300 = 600 fields, none of the dropped
	// f0..f299, and the new g fields are live.
	if h.card() != 600 {
		t.Fatalf("post-churn card = %d, want 600", h.card())
	}
	if h.has([]byte("f0")) || !h.has([]byte("g0")) {
		t.Fatal("post-churn membership wrong: f0 should be gone, g0 present")
	}
}

// --- command level (buffered path) ----------------------------------------

// TestHgetallInline checks the inline band returns the whole hash as flat pairs in
// insertion order, the listpack parity the differential also pins.
func TestHgetallInline(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1", "b", "2", "c", "3")
	wantArray(t, do(t, c, opHgetall, "h"), "a", "1", "b", "2", "c", "3")
	wantArray(t, do(t, c, opHkeys, "h"), "a", "b", "c")
	wantArray(t, do(t, c, opHvals, "h"), "1", "2", "3")
}

// TestEnumEmptyKey checks a missing key is the empty array on all three verbs, not
// a nil and not an error.
func TestEnumEmptyKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantArray(t, do(t, c, opHgetall, "missing"))
	wantArray(t, do(t, c, opHkeys, "missing"))
	wantArray(t, do(t, c, opHvals, "missing"))
}

// TestHgetallNativeBuffered drives a promoted hash whose reply still fits a chunk
// through the buffered path and checks the union is exactly the field set, each
// field paired with its value.
func TestHgetallNativeBuffered(t *testing.T) {
	c := newHarness(t).NewConn()
	const n = 600 // native, but the reply is a few KB, well under the stream cutover
	for i := 0; i < n; i++ {
		do(t, c, opHset, "h", "f"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "hashtable")

	arr := decodeReply(t, do(t, c, opHgetall, "h")).([]any)
	if len(arr) != 2*n {
		t.Fatalf("HGETALL framed %d elements, want %d", len(arr), 2*n)
	}
	got := map[string]string{}
	for i := 0; i < len(arr); i += 2 {
		got[arr[i].(string)] = arr[i+1].(string)
	}
	if len(got) != n {
		t.Fatalf("HGETALL returned %d distinct fields, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if f := "f" + strconv.Itoa(i); got[f] != "v"+strconv.Itoa(i) {
			t.Fatalf("field %q = %q, want %q", f, got[f], "v"+strconv.Itoa(i))
		}
	}
}

// TestEnumTotalMatchesFrame checks the byte width enumTotal computes equals the
// bytes the stream actually frames, for every mode, since that width is what the
// handler commits the wire to before the first chunk goes out. A cutover-sized
// hash makes the width worth trusting.
func TestEnumTotalMatchesFrame(t *testing.T) {
	// A field-value pair wide enough that a few thousand fields clear the stream
	// cutover, so this also exercises a genuinely large frame.
	pairs := make([][2]string, 3000)
	for i := range pairs {
		pairs[i] = [2]string{"field:" + strconv.Itoa(i), "value-" + strconv.Itoa(i)}
	}
	h := buildNative(pairs)
	for _, mode := range []enumMode{enumPairs, enumKeys, enumVals} {
		total := h.ft.enumTotal(mode)
		if mode == enumPairs && total <= store.ChunkSize {
			t.Fatalf("HGETALL total %d did not clear the %d stream cutover", total, store.ChunkSize)
		}
		src := h.ft.pinEnumStream(mode)
		out := drainEnum(t, src, total)
		src.Release()
		if int64(len(out)) != total {
			t.Fatalf("mode %d: framed %d bytes, enumTotal said %d", mode, len(out), total)
		}
	}
}
