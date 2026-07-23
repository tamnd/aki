package sqlo1b

// Compressed-frame group tests (doc 03 section 7): the frame codec
// itself, the compaction output stream that writes gen-C extents, the
// read dispatch through the extent-eflags cache, recompaction of a
// frame extent, and the scrub branch.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// TestCFrameRoundTrip appends random records at both group
// capacities, parses every intermediate image (the flushCompactGroup
// shape), and holds each slot to byte-exact reads.
func TestCFrameRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for _, capacity := range []int{Group0Payload, GroupSize} {
		g := NewCGroupBuilder(capacity)
		var recs [][]byte
		for {
			rec := make([]byte, 1+rng.Intn(300))
			rng.Read(rec)
			if !g.Fits(len(rec)) {
				break
			}
			slot, err := g.Append(rec)
			if err != nil {
				t.Fatal(err)
			}
			if int(slot) != len(recs) {
				t.Fatalf("slot %d, want %d", slot, len(recs))
			}
			recs = append(recs, rec)
			// Parse the growing image at every step: rewrites of the
			// open group must stay readable.
			v, err := ParseCGroup(g.Image())
			if err != nil {
				t.Fatalf("intermediate image with %d records: %v", len(recs), err)
			}
			if v.Records() != len(recs) {
				t.Fatalf("view has %d records, want %d", v.Records(), len(recs))
			}
		}
		img := g.Close()
		if len(img) != capacity {
			t.Fatalf("image of %d bytes at capacity %d", len(img), capacity)
		}
		v, err := ParseCGroup(img)
		if err != nil {
			t.Fatal(err)
		}
		if v.Scheme() != SchemeRaw {
			t.Fatalf("scheme %d, want raw", v.Scheme())
		}
		for i, want := range recs {
			got, err := v.Record(uint16(i))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("slot %d mismatch", i)
			}
		}
		if _, err := v.Record(uint16(len(recs))); err == nil {
			t.Fatal("read past the last slot succeeded")
		}
	}
}

// TestCFrameBuilderBounds pins the Fits accounting: header plus
// payload plus the grown slot table never exceeds capacity, appends
// past Fits fail, and empty records are refused.
func TestCFrameBuilderBounds(t *testing.T) {
	g := NewCGroupBuilder(GroupSize)
	if g.Fits(0) {
		t.Fatal("zero-length record fits")
	}
	if _, err := g.Append(nil); err == nil {
		t.Fatal("empty append succeeded")
	}
	if !g.Fits(GroupSize - CFrameHeader - 2) {
		t.Fatal("max single record does not fit an empty frame")
	}
	if g.Fits(GroupSize - CFrameHeader - 1) {
		t.Fatal("over-capacity record fits")
	}
	n := 0
	for g.Fits(100) {
		if _, err := g.Append(make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		n++
	}
	if used := CFrameHeader + 100*n + 2*n; used > GroupSize {
		t.Fatalf("accounting: %d bytes packed into %d", used, GroupSize)
	}
	if _, err := g.Append(make([]byte, 100)); err == nil {
		t.Fatal("append past Fits succeeded")
	}
}

// TestCFrameParseRejections drives ParseCGroup through the corrupt
// shapes it must refuse.
func TestCFrameParseRejections(t *testing.T) {
	g := NewCGroupBuilder(GroupSize)
	for range 5 {
		if _, err := g.Append([]byte("record-bytes")); err != nil {
			t.Fatal(err)
		}
	}
	good := bytes.Clone(g.Close())
	if _, err := ParseCGroup(good); err != nil {
		t.Fatal(err)
	}
	mut := func(name string, f func(b []byte)) {
		b := bytes.Clone(good)
		f(b)
		if _, err := ParseCGroup(b); err == nil {
			t.Fatalf("%s parsed", name)
		}
	}
	mut("unknown scheme", func(b []byte) { b[0] = 9 })
	mut("dict id on raw", func(b []byte) { b[1] = 3 })
	mut("clen != ulen on raw", func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 5) })
	mut("table past image", func(b []byte) { binary.LittleEndian.PutUint16(b[2:], 60000) })
	mut("clen past image", func(b []byte) { binary.LittleEndian.PutUint32(b[8:], GroupSize) })
	mut("offset out of order", func(b []byte) {
		tstart := CFrameHeader + int(binary.LittleEndian.Uint32(b[8:]))
		binary.LittleEndian.PutUint16(b[tstart+2:], 0)
	})
	mut("offset past ulen", func(b []byte) {
		tstart := CFrameHeader + int(binary.LittleEndian.Uint32(b[8:]))
		binary.LittleEndian.PutUint16(b[tstart+8:], uint16(binary.LittleEndian.Uint32(b[4:])))
	})
	mut("records in empty payload", func(b []byte) {
		binary.LittleEndian.PutUint32(b[4:], 0)
		binary.LittleEndian.PutUint32(b[8:], 0)
	})
	if _, err := ParseCGroup(make([]byte, CFrameHeader-1)); err == nil {
		t.Fatal("headerless image parsed")
	}
}

// compactAll compacts one sealed raw extent through the store and
// fails the test on error.
func (r *storeRig) compactAll(t *testing.T, ext uint64) CompactStats {
	t.Helper()
	cs, err := r.s.CompactExtent(context.Background(), ext)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// extFlagsOf reads an extent header's eflags straight off the file.
func (r *storeRig) extFlagsOf(t *testing.T, ext uint64) uint8 {
	t.Helper()
	hb := make([]byte, ExtentHeaderSize)
	if _, err := r.s.f.ReadAt(hb, int64(ext)*int64(r.s.sb.ExtentSize)); err != nil {
		t.Fatal(err)
	}
	h, err := DecodeExtentHeader(hb)
	if err != nil {
		t.Fatal(err)
	}
	return h.EFlags
}

// TestCompactWritesFrameExtents seals a raw extent, kills half its
// records, and compacts: survivors land in a compressed extent, read
// back through the frame dispatch before and after the checkpoint and
// across a reopen (where the eflags cache refills from headers), and
// the scheme histogram books raw frame groups.
func TestCompactWritesFrameExtents(t *testing.T) {
	r := newStoreRig(t)
	ext, in := r.fillSealed(t, "")
	if len(in) < 30 {
		t.Fatalf("only %d records landed in the sealed extent", len(in))
	}
	var ops []sqlo1.Op
	for i, k := range in {
		if i%2 == 0 {
			ops = append(ops, putOp(k, []byte("rewritten"), 0))
		}
	}
	r.apply(t, ops...)
	cs := r.compactAll(t, ext)
	if cs.Relocated == 0 {
		t.Fatal("nothing relocated")
	}

	cExt := r.s.cvlog.ext
	if !r.s.cvlog.active {
		t.Fatal("compact stream never activated")
	}
	if fl := r.extFlagsOf(t, cExt); fl&EFlagCompressed == 0 {
		t.Fatalf("compact extent %d eflags %#x lack EFlagCompressed", cExt, fl)
	}
	if comp, err := r.s.extCompressed(cExt); err != nil || !comp {
		t.Fatalf("extCompressed(%d) = %v, %v", cExt, comp, err)
	}
	moved := 0
	for _, k := range in {
		if r.posOf(t, k).Extent() == cExt {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("no index entry points into the compact extent")
	}
	if sg := r.s.SchemeGroups(); sg[SchemeRaw] == 0 {
		t.Fatal("no raw frame groups booked in the scheme histogram")
	}
	// Pre-checkpoint reads go through the flushed frame images.
	r.verify(t)

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.verify(t)
	// Reopen drops the eflags cache; reads refill it from headers.
	r.reopen(t)
	r.verify(t)
	if comp, err := r.s.extCompressed(cExt); err != nil || !comp {
		t.Fatalf("extCompressed(%d) after reopen = %v, %v", cExt, comp, err)
	}
}

// fillFrameExtent compacts raw extents until the compact stream rolls
// off its first extent, which seals a full frame extent, and returns
// it.
func (r *storeRig) fillFrameExtent(t *testing.T) uint64 {
	t.Helper()
	ext, _ := r.fillSealed(t, "f0-")
	r.compactAll(t, ext)
	first := r.s.cvlog.ext
	for i := 1; r.s.cvlog.ext == first; i++ {
		if i > 6 {
			t.Fatalf("compact stream never rolled off extent %d", first)
		}
		ext, _ := r.fillSealed(t, fmt.Sprintf("f%d-", i))
		r.compactAll(t, ext)
	}
	if st := r.s.grid.State(first); st != StateSealed {
		t.Fatalf("first compact extent is %s, want sealed", st)
	}
	return first
}

// TestRecompactFrameExtent seals a full frame extent, kills some of
// its records, and compacts it again: the frame walk relocates the
// survivors (gen-C to gen-C), the scrubber verifies the remaining
// sealed frame extents, and everything survives a reopen.
func TestRecompactFrameExtent(t *testing.T) {
	r := newStoreRig(t)
	first := r.fillFrameExtent(t)
	if fl := r.extFlagsOf(t, first); fl&EFlagCompressed == 0 || fl&EFlagSealed == 0 {
		t.Fatalf("frame extent eflags %#x, want compressed and sealed", fl)
	}

	var inside []string
	for k := range r.sh {
		if r.posOf(t, k).Extent() == first {
			inside = append(inside, k)
		}
	}
	if len(inside) < 10 {
		t.Fatalf("only %d live records inside the frame extent", len(inside))
	}
	var ops []sqlo1.Op
	for i, k := range inside {
		if i%2 == 0 {
			ops = append(ops, putOp(k, []byte("rewritten-2"), 0))
		}
	}
	r.apply(t, ops...)

	// Quarantined raw extents from fillFrameExtent release first so
	// the sweep below sees only settled states.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cs := r.compactAll(t, first)
	if cs.Relocated == 0 || cs.Superseded == 0 {
		t.Fatalf("frame recompaction stats %+v", cs)
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	rep := (&Scrubber{File: r.s.f, ExtentSize: r.s.sb.ExtentSize, Grid: r.s.grid}).Sweep()
	if !rep.Clean() {
		t.Fatalf("scrub findings on frame extents: %+v", rep.Findings)
	}
	if rep.Scanned == 0 {
		t.Fatal("scrub scanned nothing")
	}
	if st := r.s.grid.State(first); st != StateFree {
		t.Fatalf("recompacted frame extent is %s after the checkpoint, want free", st)
	}

	// Prime the dispatch cache off the freed extent's stale header,
	// then reuse the extent: allocStream must flip the cache to the
	// new stream's flags.
	if comp, err := r.s.extCompressed(first); err != nil || !comp {
		t.Fatalf("freed extent's stale header reads (%v, %v), want compressed", comp, err)
	}
	for i := 0; r.s.grid.State(first) == StateFree; i++ {
		if i > 6 {
			t.Fatalf("extent %d never reused by new fills", first)
		}
		r.fillSealed(t, fmt.Sprintf("r%d-", i))
	}
	if comp, err := r.s.extCompressed(first); err != nil || comp {
		t.Fatalf("reused extent %d still dispatches compressed (%v, %v)", first, comp, err)
	}
	r.verify(t)
	r.reopen(t)
	r.verify(t)
}
