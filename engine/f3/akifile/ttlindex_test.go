package akifile

import "testing"

// appendClass streams one class the way the writer will: its header, then a segment
// offset per member segment.
func appendClass(dst []byte, c TTLClass) []byte {
	dst = AppendTTLClassHeader(dst, c.Class, uint32(len(c.Segments)), c.ExpiryUpperUnix)
	for _, s := range c.Segments {
		dst = AppendTTLSegment(dst, s)
	}
	return dst
}

func encodeTTLIndex(classes []TTLClass) []byte {
	payload := AppendTTLIndexHeader(nil, TTLIndexHeader{ClassCount: uint64(len(classes))})
	for _, c := range classes {
		payload = appendClass(payload, c)
	}
	return payload
}

func eqU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTTLIndexRoundTrip builds an index over three classes, one with two segments and
// two with one each, and reads back the nested structure unchanged.
func TestTTLIndexRoundTrip(t *testing.T) {
	classes := []TTLClass{
		{Class: 8841, ExpiryUpperUnix: 1_700_000_000, Segments: []uint64{0x250000, 0x260000}},
		{Class: 8850, ExpiryUpperUnix: 1_700_003_600, Segments: []uint64{0x270000}},
		{Class: 9000, ExpiryUpperUnix: 1_800_000_000, Segments: []uint64{0x400000}},
	}
	payload := encodeTTLIndex(classes)

	h, err := ParseTTLIndexHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if h.ClassCount != 3 {
		t.Fatalf("class count = %d, want 3", h.ClassCount)
	}
	decoded, err := TTLClasses(payload, h)
	if err != nil {
		t.Fatalf("classes: %v", err)
	}
	if len(decoded) != len(classes) {
		t.Fatalf("got %d classes, want %d", len(decoded), len(classes))
	}
	for i := range classes {
		if decoded[i].Class != classes[i].Class || decoded[i].ExpiryUpperUnix != classes[i].ExpiryUpperUnix {
			t.Fatalf("class %d = %+v, want id/expiry of %+v", i, decoded[i], classes[i])
		}
		if !eqU64(decoded[i].Segments, classes[i].Segments) {
			t.Fatalf("class %d segments = %v, want %v", i, decoded[i].Segments, classes[i].Segments)
		}
	}
}

// TestExpiredSegmentsCollectsWhollyExpired confirms the reclaim signal: a class whose
// upper bound is at or below the clock contributes all its segments, a later class
// none.
func TestExpiredSegmentsCollectsWhollyExpired(t *testing.T) {
	classes := []TTLClass{
		{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000, 0x2000}},
		{Class: 2, ExpiryUpperUnix: 2000, Segments: []uint64{0x3000}}, // exactly at the clock, expired
		{Class: 3, ExpiryUpperUnix: 5000, Segments: []uint64{0x4000}}, // still live
	}
	got := ExpiredSegments(classes, 2000)
	want := []uint64{0x1000, 0x2000, 0x3000}
	if !eqU64(got, want) {
		t.Fatalf("expired segments = %v, want %v", got, want)
	}
}

// TestExpiredSegmentsNoneExpired returns nothing when every class outlives the clock.
func TestExpiredSegmentsNoneExpired(t *testing.T) {
	classes := []TTLClass{{Class: 1, ExpiryUpperUnix: 9000, Segments: []uint64{0x1000}}}
	if got := ExpiredSegments(classes, 100); got != nil {
		t.Fatalf("expired segments = %v, want none", got)
	}
}

// TestParseTTLIndexHeaderShort refuses a header buffer below the fixed size.
func TestParseTTLIndexHeaderShort(t *testing.T) {
	if _, err := ParseTTLIndexHeader(make([]byte, TTLIndexHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseTTLIndexHeaderBadMagic refuses a payload that is not a TTL index.
func TestParseTTLIndexHeaderBadMagic(t *testing.T) {
	b := make([]byte, TTLIndexHeaderLen)
	copy(b[0:4], "XXXX")
	if _, err := ParseTTLIndexHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestTTLClassesRejectsOverrunClassCount catches a class_count that claims more
// classes than the payload can hold.
func TestTTLClassesRejectsOverrunClassCount(t *testing.T) {
	classes := []TTLClass{{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000}}}
	payload := encodeTTLIndex(classes)
	le.PutUint64(payload[8:16], 1000) // claim far more classes than are present
	bad, err := ParseTTLIndexHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := TTLClasses(payload, bad); err != ErrLength {
		t.Fatalf("overrun class count err = %v, want ErrLength", err)
	}
}

// TestTTLClassesRejectsOverrunSegmentCount catches a segment_count that claims more
// offsets than the remaining bytes hold, a torn index that must not over-read.
func TestTTLClassesRejectsOverrunSegmentCount(t *testing.T) {
	classes := []TTLClass{{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000}}}
	payload := encodeTTLIndex(classes)
	// The class header's segment_count sits at the first class's offset+4.
	le.PutUint32(payload[TTLIndexHeaderLen+4:TTLIndexHeaderLen+8], 50)
	if _, err := TTLClasses(payload, TTLIndexHeader{ClassCount: 1}); err != ErrLength {
		t.Fatalf("overrun segment count err = %v, want ErrLength", err)
	}
}

// TestTTLClassesEmpty decodes an index with no classes: a header and nothing after it.
func TestTTLClassesEmpty(t *testing.T) {
	payload := AppendTTLIndexHeader(nil, TTLIndexHeader{})
	h, err := ParseTTLIndexHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	classes, err := TTLClasses(payload, h)
	if err != nil {
		t.Fatalf("classes: %v", err)
	}
	if len(classes) != 0 {
		t.Fatalf("empty index decoded %d classes", len(classes))
	}
}
