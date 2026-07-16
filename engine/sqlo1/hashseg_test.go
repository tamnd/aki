package sqlo1

import (
	"bytes"
	"fmt"
	"testing"
)

// TestHashFHPinned pins the partitioning hash byte for byte. fh is a
// format fact: fences persist fh range boundaries, so a swapped or
// reconfigured hash function would strand every segmented hash on
// disk. These are xxhash64 values.
func TestHashFHPinned(t *testing.T) {
	pins := map[string]uint64{
		"":               0xef46db3751d8e999,
		"f":              0xd00dba5cf02aee4d,
		"field1":         0x27a0678ffa856636,
		"user:1000:name": 0x4544512f96941ca3,
	}
	for field, want := range pins {
		if got := hashFH([]byte(field)); got != want {
			t.Errorf("fh(%q) = %#x, want %#x", field, got, want)
		}
	}
}

// segFromPairs builds a valid segment payload from field/value/expiry
// triples, sorting into segment order first.
func segFromPairs(t *testing.T, pairs ...any) []byte {
	t.Helper()
	if len(pairs)%3 != 0 {
		t.Fatal("segFromPairs wants (field, value, expMs) triples")
	}
	var entries []hashSegEntry
	for i := 0; i < len(pairs); i += 3 {
		f := []byte(pairs[i].(string))
		entries = append(entries, hashSegEntry{
			fh:    hashFH(f),
			field: f,
			val:   []byte(pairs[i+1].(string)),
			expMs: int64(pairs[i+2].(int)),
		})
	}
	sortHashSegEntries(entries)
	return appendHashSegPayload(nil, entries, false)
}

func TestHashSegCodec(t *testing.T) {
	p := segFromPairs(t, "a", "1", 0, "b", "2", 900, "c", "3", 500)
	s, err := decodeHashSeg(p, false)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.n != 3 || s.minExpMs != 500 {
		t.Fatalf("decoded n=%d minExp=%d, want 3, 500", s.n, s.minExpMs)
	}

	empty := appendHashSegPayload(nil, nil, false)
	if len(empty) != hashSegHdrLen {
		t.Fatalf("empty segment payload is %d bytes, want %d", len(empty), hashSegHdrLen)
	}
	if s, err := decodeHashSeg(empty, false); err != nil || s.n != 0 || s.minExpMs != 0 {
		t.Fatalf("empty segment decode: n=%d minExp=%d err=%v", s.n, s.minExpMs, err)
	}

	// Out-of-order and duplicate regions are built by hand: header
	// first, entries appended in the wrong order.
	misordered := func(fields ...string) []byte {
		b := make([]byte, hashSegHdrLen)
		for _, f := range fields {
			b = appendHashEntry(b, []byte(f), []byte("v"), 0, false)
		}
		putHashSegHdr(b, len(fields), 0)
		return b
	}
	af, bf := "a", "b"
	if !hashSegKeyLess(hashFH([]byte(af)), []byte(af), hashFH([]byte(bf)), []byte(bf)) {
		af, bf = bf, af
	}
	reserved := segFromPairs(t, "a", "1", 0)
	reserved[2] = 1
	badCount := segFromPairs(t, "a", "1", 0)
	putHashSegHdr(badCount, 2, 0)
	badMin := segFromPairs(t, "a", "1", 700)
	putHashSegHdr(badMin, 1, 300)
	corrupt := map[string][]byte{
		"short header":     {1, 0, 0},
		"reserved set":     reserved,
		"out of order":     misordered(bf, af),
		"duplicate field":  misordered("dup", "dup"),
		"count mismatch":   badCount,
		"min_expire wrong": badMin,
	}
	for name, p := range corrupt {
		if _, err := decodeHashSeg(p, false); err == nil {
			t.Errorf("%s: corrupt segment decoded cleanly", name)
		}
	}
}

// TestHashSegPointOps churns one segment against a map reference,
// re-validating the payload after every write.
func TestHashSegPointOps(t *testing.T) {
	type fv struct {
		val   string
		expMs int64
	}
	ref := map[string]fv{}
	cur := appendHashSegPayload(nil, nil, false)
	var scratch []byte

	rng := uint64(0x9e3779b97f4a7c15)
	next := func(n int) int {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return int(rng % uint64(n))
	}

	check := func(field string) {
		t.Helper()
		s, err := decodeHashSeg(cur, false)
		if err != nil {
			t.Fatalf("payload invalid after write: %v", err)
		}
		if s.n != len(ref) {
			t.Fatalf("n = %d, reference holds %d", s.n, len(ref))
		}
		f := []byte(field)
		v, expMs, ok, err := hashSegGet(s, hashFH(f), f)
		if err != nil {
			t.Fatal(err)
		}
		want, there := ref[field]
		if ok != there {
			t.Fatalf("get %q = %v, want %v", field, ok, there)
		}
		if ok && (string(v) != want.val || expMs != want.expMs) {
			t.Fatalf("get %q = (%q, %d), want (%q, %d)", field, v, expMs, want.val, want.expMs)
		}
	}

	for i := range 4000 {
		field := fmt.Sprintf("f%02d", next(48))
		f := []byte(field)
		s, err := decodeHashSeg(cur, false)
		if err != nil {
			t.Fatal(err)
		}
		switch next(3) {
		case 0, 1:
			val := fmt.Sprintf("v%d", i)
			var expMs int64
			if next(4) == 0 {
				expMs = int64(1000 + next(9000))
			}
			out, created, _, err := hashSegSet(scratch, s, hashFH(f), f, []byte(val), expMs, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, there := ref[field]
			if created == there {
				t.Fatalf("set %q created=%v, reference says %v", field, created, there)
			}
			ref[field] = fv{val: val, expMs: expMs}
			scratch, cur = cur, out
		case 2:
			out, removed, err := hashSegDel(scratch, s, hashFH(f), f, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, there := ref[field]; removed != there {
				t.Fatalf("del %q removed=%v, reference says %v", field, removed, there)
			}
			if removed {
				delete(ref, field)
				scratch, cur = cur, out
			}
		}
		check(field)
	}
}

func TestHashSegSplitMerge(t *testing.T) {
	var entries []hashSegEntry
	for i := range 80 {
		f := fmt.Appendf(nil, "field-%03d", i)
		entries = append(entries, hashSegEntry{
			fh:    hashFH(f),
			field: f,
			val:   bytes.Repeat([]byte{'x'}, 60),
			expMs: 0,
		})
	}
	sortHashSegEntries(entries)
	whole := appendHashSegPayload(nil, entries, false)
	if len(whole) <= hashSegMax {
		t.Fatalf("test segment is %d bytes, wanted past seg_max %d", len(whole), hashSegMax)
	}

	parsed, err := parseHashSegEntries(nil, whole[hashSegHdrLen:], false)
	if err != nil {
		t.Fatal(err)
	}
	mid, boundary, ok := splitHashSegEntries(parsed, 0)
	if !ok {
		t.Fatal("split refused a plain oversized segment")
	}
	if mid == 0 || mid == len(parsed) {
		t.Fatalf("split cut at %d of %d", mid, len(parsed))
	}
	for i, e := range parsed {
		if i < mid && e.fh >= boundary {
			t.Fatalf("left entry %d fh %#x at or past boundary %#x", i, e.fh, boundary)
		}
		if i >= mid && e.fh < boundary {
			t.Fatalf("right entry %d fh %#x below boundary %#x", i, e.fh, boundary)
		}
	}
	if boundary != parsed[mid].fh {
		t.Fatalf("boundary %#x is not the first right entry's fh %#x", boundary, parsed[mid].fh)
	}

	lop := appendHashSegPayload(nil, parsed[:mid], false)
	hip := appendHashSegPayload(nil, parsed[mid:], false)
	if _, err := decodeHashSeg(lop, false); err != nil {
		t.Fatalf("left half: %v", err)
	}
	if _, err := decodeHashSeg(hip, false); err != nil {
		t.Fatalf("right half: %v", err)
	}

	merged, err := mergeHashSegs(nil, lop, hip, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(merged, whole) {
		t.Fatal("merge of the two halves does not reproduce the original payload")
	}
	if _, err := mergeHashSegs(nil, hip, lop, false); err == nil {
		t.Fatal("merge accepted halves in the wrong range order")
	}

	small := segFromPairs(t, "a", "1", 0, "b", "2", 0)
	if !shouldMergeHashSegs(len(small), len(small)) {
		t.Fatal("two tiny segments did not qualify for the lazy merge")
	}
	if shouldMergeHashSegs(len(lop), len(hip)) {
		t.Fatal("two half-full segments qualified for the lazy merge")
	}

	// Field TTLs ride the merge: min_expire of the merged segment is
	// the smaller of the halves'.
	tl := segFromPairs(t, "a", "1", 800)
	th := segFromPairs(t, "b", "2", 300)
	lo, hi := tl, th
	if hashSegKeyLess(hashFH([]byte("b")), []byte("b"), hashFH([]byte("a")), []byte("a")) {
		lo, hi = th, tl
	}
	mergedTTL, err := mergeHashSegs(nil, lo, hi, false)
	if err != nil {
		t.Fatal(err)
	}
	ms, err := decodeHashSeg(mergedTTL, false)
	if err != nil {
		t.Fatal(err)
	}
	if ms.minExpMs != 300 {
		t.Fatalf("merged min_expire = %d, want 300", ms.minExpMs)
	}
}

// TestHashSegSplitGuards exercises the refusal paths with hand-built
// fh values, which real xxhash collisions cannot produce on demand.
func TestHashSegSplitGuards(t *testing.T) {
	ent := func(fh uint64) hashSegEntry {
		return hashSegEntry{fh: fh, field: []byte("f"), val: []byte("v")}
	}
	if _, _, ok := splitHashSegEntries([]hashSegEntry{ent(7), ent(7), ent(7), ent(7)}, 0); ok {
		t.Fatal("split accepted a segment of one fh value")
	}
	if _, _, ok := splitHashSegEntries([]hashSegEntry{ent(3), ent(4), ent(5), ent(6)}, 5); ok {
		t.Fatal("split accepted a boundary at or below the segment's lo")
	}
	if mid, boundary, ok := splitHashSegEntries([]hashSegEntry{ent(1), ent(2), ent(2), ent(2)}, 0); !ok || mid != 1 || boundary != 2 {
		t.Fatalf("equal-fh run walk-back: mid=%d boundary=%d ok=%v, want 1, 2, true", mid, boundary, ok)
	}
}

// TestHashSegUpgradeOrder is the inline-to-segmented re-sort: parse an
// insertion-order region, sort, encode, and get a valid segment
// holding the same fields.
func TestHashSegUpgradeOrder(t *testing.T) {
	var region []byte
	region = appendHashEntry(region, []byte("zeta"), []byte("1"), 0, false)
	region = appendHashEntry(region, []byte("alpha"), []byte("2"), 400, false)
	region = appendHashEntry(region, []byte("mid"), []byte("3"), 0, false)

	entries, err := parseHashSegEntries(nil, region, false)
	if err != nil {
		t.Fatal(err)
	}
	sortHashSegEntries(entries)
	p := appendHashSegPayload(nil, entries, false)
	s, err := decodeHashSeg(p, false)
	if err != nil {
		t.Fatalf("upgraded segment invalid: %v", err)
	}
	if s.n != 3 || s.minExpMs != 400 {
		t.Fatalf("upgraded segment n=%d minExp=%d, want 3, 400", s.n, s.minExpMs)
	}
	for _, f := range []string{"zeta", "alpha", "mid"} {
		fb := []byte(f)
		if _, _, ok, err := hashSegGet(s, hashFH(fb), fb); err != nil || !ok {
			t.Fatalf("upgraded segment lost %q: ok=%v err=%v", f, ok, err)
		}
	}
}
