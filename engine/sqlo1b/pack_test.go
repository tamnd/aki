package sqlo1b

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// packRecord encodes one RecString record for builder-level tests.
func packRecord(t testing.TB, i int, val []byte) []byte {
	t.Helper()
	rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "pk-%04d", i), Value: val}
	b, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAppendPackedBuilder pins the builder contract: compressible
// records pack far past the raw projection, the packed image stays at
// capacity and round-trips byte-exact, and a record the certified
// image cannot absorb reverts without corrupting the group.
func TestAppendPackedBuilder(t *testing.T) {
	g := NewCGroupBuilder(GroupSize)
	val := bytes.Repeat([]byte{'v'}, 300)
	var recs [][]byte
	rawFit := 0
	for i := 0; ; i++ {
		b := packRecord(t, i, val)
		if g.Fits(len(b)) {
			if _, err := g.Append(b); err != nil {
				t.Fatal(err)
			}
			recs = append(recs, b)
			rawFit++
			continue
		}
		slot, ok, err := g.AppendPacked(b)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if int(slot) != len(recs) {
			t.Fatalf("packed slot %d, want %d", slot, len(recs))
		}
		recs = append(recs, b)
	}
	if len(recs) < 2*rawFit {
		t.Fatalf("packing added %d records over the raw projection's %d", len(recs)-rawFit, rawFit)
	}

	img := g.Seal()
	if len(img) != GroupSize {
		t.Fatalf("packed image is %d bytes, want the %d group", len(img), GroupSize)
	}
	if g.Scheme() == SchemeRaw {
		t.Fatal("packed group sealed raw")
	}
	v, err := ParseCGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if v.Records() != len(recs) {
		t.Fatalf("packed frame parses %d records, want %d", v.Records(), len(recs))
	}
	for i, want := range recs {
		got, err := v.Record(uint16(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("packed record %d diverged: %v", i, err)
		}
	}
}

// TestAppendPackedRevert pins the refusal paths: incompressible
// payloads never pack, a failed pack leaves the group byte-identical,
// and the ulen cap holds.
func TestAppendPackedRevert(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	noise := func() []byte {
		b := make([]byte, 300)
		rng.Read(b)
		return b
	}
	g := NewCGroupBuilder(GroupSize)
	n := 0
	for {
		b := packRecord(t, n, noise())
		if !g.Fits(len(b)) {
			if _, ok, err := g.AppendPacked(b); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatal("high-entropy record packed past the raw projection")
			}
			break
		}
		if _, err := g.Append(b); err != nil {
			t.Fatal(err)
		}
		n++
	}
	// The reverted append left no trace: the raw image still parses
	// to exactly the accepted records.
	v, err := ParseCGroup(g.Image())
	if err != nil {
		t.Fatal(err)
	}
	if v.Records() != n {
		t.Fatalf("reverted pack left %d records, want %d", v.Records(), n)
	}

	// The ulen cap refuses before any encode work.
	packed := NewCGroupBuilder(GroupSize)
	big := packRecord(t, 0, bytes.Repeat([]byte{'v'}, 2000))
	for packed.Fits(len(big)) {
		if _, err := packed.Append(big); err != nil {
			t.Fatal(err)
		}
	}
	for {
		_, ok, err := packed.AppendPacked(big)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	if packed.used+len(big) <= cframeMaxUlen {
		t.Fatalf("packing stopped at %d used with %d-byte records and room under the cap", packed.used, len(big))
	}
}

// TestCompactPacksGroups drives packing through the store: constant
// values compact into frame groups holding several raw projections'
// worth of records, so the same content spans fewer groups, which is
// the G3 disk lever. Verify runs across checkpoint and reopen.
func TestCompactPacksGroups(t *testing.T) {
	r := newStoreRig(t)
	ext, keys := r.fillSealed(t, "pk-")
	if _, err := r.s.CompactExtent(context.Background(), ext); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Count records per compact frame group. 950-byte records fit 4
	// to a raw group; packing must beat that on every closed group
	// the constants landed in.
	perGroup := map[uint64]int{}
	for _, k := range keys {
		pos := r.posOf(t, k)
		if pos.IsBlob() {
			continue
		}
		perGroup[pos.Extent()<<16|uint64(pos.Group())]++
	}
	if len(perGroup) == 0 {
		t.Fatal("no relocated records landed in frame groups")
	}
	best := 0
	for _, n := range perGroup {
		if n > best {
			best = n
		}
	}
	rec := &Record{RType: RecString, Key: []byte(keys[len(keys)-1]), Value: bytes.Repeat([]byte{'v'}, 950)}
	enc, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	rawBound := (GroupSize - CFrameHeader) / (len(enc) + 2)
	if best <= 2*rawBound {
		t.Fatalf("densest packed group holds %d records, raw bound is ~%d", best, rawBound)
	}

	r.verify(t)
	r.reopen(t)
	r.verify(t)
}

// TestPackedFlushThroughReads pins the stale-view guard: reads off
// the open packed group's flushed image cache a non-raw view, a later
// compaction rewrites the same group, and reads of the newly packed
// records must see the fresh image, not the cached one.
func TestPackedFlushThroughReads(t *testing.T) {
	r := newStoreRig(t)
	extA, keysA := r.fillSealed(t, "fa-")
	if _, err := r.s.CompactExtent(context.Background(), extA); err != nil {
		t.Fatal(err)
	}
	// Read a relocated key: if the open group flushed packed, this
	// caches its current non-raw image.
	if _, err := r.s.Get(context.Background(), []byte(keysA[len(keysA)-1])); err != nil {
		t.Fatal(err)
	}
	extB, keysB := r.fillSealed(t, "fb-")
	if _, err := r.s.CompactExtent(context.Background(), extB); err != nil {
		t.Fatal(err)
	}
	// Records from the second pass may share the rewritten group; a
	// stale cached view would miss their slots.
	for _, k := range keysB {
		if _, err := r.s.Get(context.Background(), []byte(k)); err != nil {
			t.Fatalf("read of %q after open-group rewrite: %v", k, err)
		}
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}
