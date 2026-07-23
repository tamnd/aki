package sqlo1b

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// sealedFrameImage builds one sealed frame group full of the given
// values and returns the image plus the appended record bytes.
func sealedFrameImage(t testing.TB, value func(i int) []byte) ([]byte, [][]byte) {
	t.Helper()
	g := NewCGroupBuilder(GroupSize)
	var recs [][]byte
	for i := 0; ; i++ {
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "fc-%03d", i), Value: value(i)}
		b, err := rec.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if !g.Fits(len(b)) {
			break
		}
		if _, err := g.Append(b); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, b)
	}
	return g.Seal(), recs
}

// TestFrameCacheView pins the cache contract: non-raw frames decode
// once and hit after, raw frames bypass, eviction is oldest-first,
// and DropExtent kills exactly one extent's views.
func TestFrameCacheView(t *testing.T) {
	img, recs := sealedFrameImage(t, func(int) []byte { return bytes.Repeat([]byte{'v'}, 300) })
	if img[0] == SchemeRaw {
		t.Fatalf("constant values sealed raw, the fixture needs a coded frame")
	}
	fc := NewFrameCache()
	v1, err := fc.View(7, 3, img)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := fc.View(7, 3, img)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != v2 {
		t.Error("second view of the same frame was not the cached one")
	}
	if got, err := v2.Record(0); err != nil || !bytes.Equal(got, recs[0]) {
		t.Fatalf("cached view record diverged: %v", err)
	}
	if fs := fc.Stats(); fs.Decodes != 1 || fs.Hits != 1 || fs.DecodeBytes == 0 {
		t.Fatalf("stats after one decode and one hit: %+v", fs)
	}

	// A raw frame never enters the cache, so re-viewing it books
	// neither a decode nor a hit.
	rg := NewCGroupBuilder(GroupSize)
	if _, err := rg.Append(recs[0]); err != nil {
		t.Fatal(err)
	}
	raw := rg.Image()
	for range 2 {
		if _, err := fc.View(7, 9, raw); err != nil {
			t.Fatal(err)
		}
	}
	if fs := fc.Stats(); fs.Decodes != 1 || fs.Hits != 1 {
		t.Fatalf("raw views touched the counters: %+v", fs)
	}

	// Fill past capacity: the oldest entry evicts, everything else
	// still hits.
	for i := range frameCacheSize {
		if _, err := fc.View(100+uint64(i), 0, img); err != nil {
			t.Fatal(err)
		}
	}
	before := fc.Stats()
	if _, err := fc.View(7, 3, img); err != nil {
		t.Fatal(err)
	}
	if fs := fc.Stats(); fs.Decodes != before.Decodes+1 {
		t.Fatalf("evicted frame served a hit: %+v -> %+v", before, fs)
	}

	// DropExtent removes the one extent and leaves the rest cached.
	fc.DropExtent(100)
	before = fc.Stats()
	if _, err := fc.View(101, 0, img); err != nil {
		t.Fatal(err)
	}
	if _, err := fc.View(100, 0, img); err != nil {
		t.Fatal(err)
	}
	fs := fc.Stats()
	if fs.Hits != before.Hits+1 || fs.Decodes != before.Decodes+1 {
		t.Fatalf("DropExtent evicted the wrong views: %+v -> %+v", before, fs)
	}

	// A nil cache is the passthrough shape readers without a store use.
	var nilFC *FrameCache
	if _, err := nilFC.View(1, 1, img); err != nil {
		t.Fatal(err)
	}
	nilFC.DropExtent(1)
	if fs := nilFC.Stats(); fs != (FrameStats{}) {
		t.Fatalf("nil cache reported stats %+v", fs)
	}
}

// TestFrameCacheReadPath drives the cache through the store: a batch
// whose keys share one frame group decodes the payload once, repeat
// point reads hit, and the counters surface through Stats.
func TestFrameCacheReadPath(t *testing.T) {
	r := newStoreRig(t)
	ext, keys := r.fillSealed(t, "fc-")
	if _, err := r.s.CompactExtent(context.Background(), ext); err != nil {
		t.Fatal(err)
	}
	// Point reads must go cold through the IndexReader, not the dirty
	// map or the pending buffer.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Collect keys landing in one compressed frame group.
	groups := map[uint64][][]byte{}
	var big uint64
	for _, k := range keys {
		pos := r.posOf(t, k)
		if pos.IsBlob() {
			continue
		}
		gk := pos.Extent()<<16 | uint64(pos.Group())
		groups[gk] = append(groups[gk], []byte(k))
		if len(groups[gk]) > len(groups[big]) {
			big = gk
		}
	}
	batch := groups[big]
	if len(batch) < 2 {
		t.Fatalf("no frame group holds two keys, groups %d", len(groups))
	}

	st0 := r.s.Stats()
	recs, err := r.s.BatchGet(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	for i, rec := range recs {
		if rec.Key == nil {
			t.Fatalf("batch key %q missed", batch[i])
		}
	}
	st1 := r.s.Stats()
	if d := st1.FrameDecodes - st0.FrameDecodes; d != 1 {
		t.Fatalf("one-group batch of %d keys paid %d decodes", len(batch), d)
	}
	if h := st1.FrameHits - st0.FrameHits; h != int64(len(batch))-1 {
		t.Fatalf("one-group batch of %d keys booked %d hits", len(batch), h)
	}

	// The repeat point read hits the still-cached view.
	if _, err := r.s.Get(context.Background(), batch[0]); err != nil {
		t.Fatal(err)
	}
	st2 := r.s.Stats()
	if st2.FrameDecodes != st1.FrameDecodes || st2.FrameHits != st1.FrameHits+1 {
		t.Fatalf("repeat point read: %+v -> %+v", st1, st2)
	}
	if st2.FrameDecodeBytes == st0.FrameDecodeBytes {
		t.Fatal("decode bytes never moved")
	}
	r.verify(t)
	r.reopen(t)
	r.verify(t)
}

// BenchmarkFrameRead measures the point-read decode cost per scheme,
// cold (every read parses the frame) against cached (the FrameCache
// serves the decoded view). The cold-minus-cached gap is the latency
// the cache removes from same-group reads.
func BenchmarkFrameRead(b *testing.B) {
	shapes := []struct {
		name  string
		value func(i int) []byte
	}{
		{"dict", func(int) []byte { return bytes.Repeat([]byte{'v'}, 300) }},
		{"zstd", func(i int) []byte { return zstdJSONValue(i, 300) }},
	}
	for _, sh := range shapes {
		img, recs := sealedFrameImage(b, sh.value)
		b.Run(sh.name+"/cold", func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				v, err := ParseCGroup(img)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := v.Record(uint16(i % len(recs))); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(sh.name+"/cached", func(b *testing.B) {
			fc := NewFrameCache()
			for i := 0; b.Loop(); i++ {
				v, err := fc.View(1, 0, img)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := v.Record(uint16(i % len(recs))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
