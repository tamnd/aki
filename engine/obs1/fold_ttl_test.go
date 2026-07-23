package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The TTL projection slice (spec 2064/obs1 doc 03 section 5.1): record
// deadlines staged by the drain flow into per-chunk expiry bounds in the
// segment footer, a segment where every record bears a deadline earns a
// retirement class at fold time, and the class rides the ledger row into
// the manifest. The class is an optimization, never authority: readers keep
// checking the inline deadlines, so these tests hold the projection's
// bookkeeping, not correctness of expiry itself.

// ttlFrames builds a staged buffer of embedded string records with
// per-record deadlines; a zero deadline stages a deathless record.
func ttlFrames(keys, vals []string, exps []uint64) []byte {
	var buf []byte
	for i := range keys {
		buf = store.AppendRecordFrame(buf, kindString, 0, uint32(len(vals[i])), []byte(keys[i]), []byte(vals[i]), exps[i])
	}
	return buf
}

// TestTTLClassOf pins the window math: due deadlines land in class 1, the
// next 24 hours map to hourly classes 1..24, days after that count from 25,
// and the byte caps at 255.
func TestTTLClassOf(t *testing.T) {
	const now = uint64(1_700_000_000_000)
	const hour = uint64(3_600_000)
	const day = 24 * hour
	cases := []struct {
		max  uint64
		want uint8
	}{
		{0, 0},
		{now - 1, 1},
		{now, 1},
		{now + 1, 1},
		{now + hour - 1, 1},
		{now + hour, 2},
		{now + 2*hour, 3},
		{now + 24*hour - 1, 24},
		{now + 24*hour, 25},
		{now + 2*day - 1, 25},
		{now + 2*day, 26},
		{now + 231*day, 255},
		{now + 232*day, 255},
		{^uint64(0), 255},
	}
	for _, c := range cases {
		if got := obs1.TTLClassOf(c.max, now); got != c.want {
			t.Errorf("TTLClassOf(now%+d ms) = %d, want %d", int64(c.max)-int64(now), got, c.want)
		}
	}
}

// TestSegmentTTLBoundsRoundTrip holds the codec facts: per-chunk expiry
// bounds and the footer's class survive encode and parse byte for byte, and
// the shape validation refuses bounds that lie.
func TestSegmentTTLBoundsRoundTrip(t *testing.T) {
	footer := obs1.SegmentFooter{
		Group: 5, Epoch: 3, SegSeq: 11, Level: 0,
		TTLClass: 3, MinExpMS: 1000, MaxExpMS: 9000,
	}
	chunks := []obs1.SegmentChunk{
		{Key: []byte("a"), Kind: 1, Count: 2, LiveHint: 2, MinExpMS: 1000, MaxExpMS: 4000, Data: chunkFrameT(64)},
		{Key: []byte("b"), Kind: 1, FirstDisc: 7, Count: 3, LiveHint: 3, MinExpMS: 2500, MaxExpMS: 9000, Data: chunkFrameT(80)},
	}
	keys := [][]byte{[]byte("a"), []byte("b")}
	seg, err := obs1.BuildSegment(footer, chunks, keys, 0)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := obs1.AppendSegment(nil, 9, seg)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatal(err)
	}
	f := again.Footer
	if f.TTLClass != 3 || f.MinExpMS != 1000 || f.MaxExpMS != 9000 {
		t.Fatalf("footer class %d bounds %d..%d after the round trip", f.TTLClass, f.MinExpMS, f.MaxExpMS)
	}
	if f.Chunks[0].MinExpMS != 1000 || f.Chunks[0].MaxExpMS != 4000 ||
		f.Chunks[1].MinExpMS != 2500 || f.Chunks[1].MaxExpMS != 9000 {
		t.Fatalf("chunk bounds after the round trip: %+v", f.Chunks)
	}

	// A max bound with no min aliases "no bearers"; the encoder refuses.
	seg.Footer.Chunks[0].MinExpMS = 0
	if _, err := obs1.AppendSegment(nil, 9, seg); err == nil {
		t.Fatal("a chunk with a max expiry bound but no min encoded")
	}
	seg.Footer.Chunks[0].MinExpMS = 5000
	seg.Footer.Chunks[0].MaxExpMS = 4999
	if _, err := obs1.AppendSegment(nil, 9, seg); err == nil {
		t.Fatal("backward chunk expiry bounds encoded")
	}
	seg.Footer.Chunks[0].MinExpMS, seg.Footer.Chunks[0].MaxExpMS = 1000, 4000

	// Footer-level rules: class 0 carries no bounds, a nonzero class needs
	// ordered nonzero bounds.
	seg.Footer.TTLClass = 0
	if _, err := obs1.AppendSegment(nil, 9, seg); err == nil {
		t.Fatal("class 0 with expiry bounds encoded")
	}
	seg.Footer.TTLClass, seg.Footer.MinExpMS = 3, 0
	if _, err := obs1.AppendSegment(nil, 9, seg); err == nil {
		t.Fatal("a classed segment with no min bound encoded")
	}
}

// chunkFrameT builds an f3-shaped chunk frame: leading total u32, filler.
func chunkFrameT(n int) []byte {
	b := []byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}
	for len(b) < n {
		b = append(b, byte(len(b)))
	}
	return b
}

// TestFolderTTLProjection folds records that all bear deadlines and reads
// the projection back off the object: every run chunk's entry carries its
// records' bounds, the footer aggregates them, the class matches the fold
// clock's window for the latest deadline, and the ledger row repeats the
// footer.
func TestFolderTTLProjection(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 64) // small run target so the records span chunks
	base := uint64(time.Now().UnixMilli())
	const hour = uint64(3_600_000)

	keys := make([]string, 10)
	vals := make([]string, 10)
	exps := make([]uint64, 10)
	for i := range keys {
		keys[i] = string(rune('a' + i))
		vals[i] = "value-0"
		exps[i] = base + 2*hour + uint64(i)*hour // 2h..11h out, min first
	}
	minExp, maxExp := exps[0], exps[9]
	fx.folder.Add(ttlFrames(keys, vals, exps))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	led := fx.folder.Ledger()[0]
	if led.TTLClass != 12 || led.MinExpMS != minExp || led.MaxExpMS != maxExp {
		t.Fatalf("ledger class %d bounds %d..%d, want 12 and %d..%d", led.TTLClass, led.MinExpMS, led.MaxExpMS, minExp, maxExp)
	}
	obj, _, err := fx.sim.Get(ctx, led.Key)
	if err != nil {
		t.Fatal(err)
	}
	seg, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatal(err)
	}
	f := seg.Footer
	if f.TTLClass != led.TTLClass || f.MinExpMS != minExp || f.MaxExpMS != maxExp {
		t.Fatalf("footer class %d bounds %d..%d disagree with the ledger", f.TTLClass, f.MinExpMS, f.MaxExpMS)
	}
	if len(f.Chunks) < 2 {
		t.Fatalf("%d chunks, want the records spanning at least 2", len(f.Chunks))
	}
	var lo, hi uint64
	for i, c := range f.Chunks {
		if c.MinExpMS == 0 || c.MinExpMS > c.MaxExpMS {
			t.Fatalf("chunk %d bounds %d..%d", i, c.MinExpMS, c.MaxExpMS)
		}
		if lo == 0 || c.MinExpMS < lo {
			lo = c.MinExpMS
		}
		if c.MaxExpMS > hi {
			hi = c.MaxExpMS
		}
	}
	if lo != minExp || hi != maxExp {
		t.Fatalf("chunk bounds aggregate to %d..%d, want %d..%d", lo, hi, minExp, maxExp)
	}
}

// TestFolderTTLMixedKeepsClassZero pins the all-bearers rule: one deathless
// record keeps the segment at class 0 with no footer bounds, while the run
// chunk's entry still publishes the bearer's bounds for planners.
func TestFolderTTLMixedKeepsClassZero(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)
	exp := uint64(time.Now().UnixMilli()) + 3_600_000

	fx.folder.Add(ttlFrames([]string{"dies", "stays"}, []string{"v1", "v2"}, []uint64{exp, 0}))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	led := fx.folder.Ledger()[0]
	if led.TTLClass != 0 || led.MinExpMS != 0 || led.MaxExpMS != 0 {
		t.Fatalf("mixed fold published class %d bounds %d..%d, want all zero", led.TTLClass, led.MinExpMS, led.MaxExpMS)
	}
	obj, _, err := fx.sim.Get(ctx, led.Key)
	if err != nil {
		t.Fatal(err)
	}
	seg, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatal(err)
	}
	f := seg.Footer
	if f.TTLClass != 0 || f.MinExpMS != 0 || f.MaxExpMS != 0 {
		t.Fatalf("mixed footer class %d bounds %d..%d, want all zero", f.TTLClass, f.MinExpMS, f.MaxExpMS)
	}
	if len(f.Chunks) != 1 || f.Chunks[0].MinExpMS != exp || f.Chunks[0].MaxExpMS != exp {
		t.Fatalf("chunk entry %+v, want the bearer's bounds %d..%d", f.Chunks, exp, exp)
	}
}

// TestFolderHashChunkTTLBounds drives the collection path: a demoter chunk
// with the #1294 TTL bitmap walks its packed pairs once at intake, and the
// field deadlines land as the chunk entry's bounds; with every field a
// bearer, the segment classes too.
func TestFolderHashChunkTTLBounds(t *testing.T) {
	ctx := context.Background()
	fx := newFoldFixture(t, 0)
	base := uint64(time.Now().UnixMilli())
	const hour = uint64(3_600_000)

	fields := []hfield{
		{name: "f-early", val: "v1", exp: base + hour},
		{name: "f-mid", val: "v2", exp: base + 2*hour},
		{name: "f-late", val: "v3", exp: base + 2*hour + 30*60_000},
	}
	minExp, maxExp := base+hour, base+2*hour+30*60_000
	fx.folder.Add(hashChunkFrames("h1", fields, len(fields)))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	led := fx.folder.Ledger()[0]
	if led.TTLClass != 3 || led.MinExpMS != minExp || led.MaxExpMS != maxExp {
		t.Fatalf("ledger class %d bounds %d..%d, want 3 and %d..%d", led.TTLClass, led.MinExpMS, led.MaxExpMS, minExp, maxExp)
	}
	obj, _, err := fx.sim.Get(ctx, led.Key)
	if err != nil {
		t.Fatal(err)
	}
	seg, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatal(err)
	}
	f := seg.Footer
	if len(f.Chunks) != 1 || f.Chunks[0].MinExpMS != minExp || f.Chunks[0].MaxExpMS != maxExp {
		t.Fatalf("collection chunk entry %+v, want bounds %d..%d", f.Chunks, minExp, maxExp)
	}
	if f.TTLClass != 3 || f.MinExpMS != minExp || f.MaxExpMS != maxExp {
		t.Fatalf("footer class %d bounds %d..%d", f.TTLClass, f.MinExpMS, f.MaxExpMS)
	}
}

// TestManifestRowCarriesTTLClass follows the class one hop further: the
// publisher's manifest row repeats the ledger's class and bounds, which is
// what the retirement scan will plan from.
func TestManifestRowCarriesTTLClass(t *testing.T) {
	fx := newPubFixture(t, nil)
	base := uint64(time.Now().UnixMilli())
	const hour = uint64(3_600_000)

	fx.folder.Add(ttlFrames([]string{"k"}, []string{"v"}, []uint64{base + 5*hour}))
	fx.folder.Flush()
	waitFor(t, "manifest", func() bool { return len(fx.manifests()) == 1 })

	m := fx.manifests()[0]
	if len(m.Segs) != 1 {
		t.Fatalf("manifest carries %d segments, want 1", len(m.Segs))
	}
	row := m.Segs[0]
	if row.TTLClass != 6 || row.MinExpMS != base+5*hour || row.MaxExpMS != base+5*hour {
		t.Fatalf("manifest row class %d bounds %d..%d, want 6 at %d", row.TTLClass, row.MinExpMS, row.MaxExpMS, base+5*hour)
	}
}
