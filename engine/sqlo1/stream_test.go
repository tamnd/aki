package sqlo1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

// streamRig is the layer test rig: the stream layer over the recording
// store, a fake clock for the auto-ID paths, and a model of the live
// entries the invariant check and the range oracle compare against.
type streamRig struct {
	t     *testing.T
	rs    *recordingStore
	tr    *Tiered
	x     *Stream
	nowMs int64
	model []streamModelEnt
}

type streamModelEnt struct {
	id streamID
	fv [][]byte
}

func newStreamRig(t *testing.T) *streamRig {
	t.Helper()
	r := &streamRig{t: t, rs: newRecordingStore(), nowMs: 1_000_000}
	r.tr = NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     11,
		NowMs:    func() int64 { return r.nowMs },
	})
	x, err := NewStream(r.tr, StreamConfig{})
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	r.x = x
	return r
}

// add lands one XADD, records it in the model, and runs the full
// invariant check, so every test sequence audits the on-store shape at
// every step.
func (r *streamRig) add(key string, mode int, req streamID, fv ...string) streamID {
	r.t.Helper()
	bs := make([][]byte, len(fv))
	for i, f := range fv {
		bs[i] = []byte(f)
	}
	id, ok, err := r.x.Add(context.Background(), []byte(key), mode, req, r.nowMs, false, bs)
	if err != nil {
		r.t.Fatalf("Add(%q, %v): %v", key, fv, err)
	}
	if !ok {
		r.t.Fatalf("Add(%q) reported the NOMKSTREAM miss without NOMKSTREAM", key)
	}
	if len(r.model) > 0 && !r.model[len(r.model)-1].id.less(id) {
		r.t.Fatalf("Add(%q) generated %v, not above the previous %v", key, id, r.model[len(r.model)-1].id)
	}
	r.model = append(r.model, streamModelEnt{id: id, fv: bs})
	r.check(key)
	return id
}

// check audits key's whole persisted shape against the codec and the
// model: the root decodes, every run walks clean and re-encodes
// byte-identically (the incremental tail amendment checked against a
// from-scratch encode, the slice 1 seam), the fence bases and counts
// match the runs, IDs stay strictly increasing across runs, and the
// live entries equal the model exactly.
func (r *streamRig) check(key string) {
	r.t.Helper()
	ctx := context.Background()
	v, isRoot, _, ok, err := r.tr.LookupEntry(ctx, []byte(key))
	if err != nil || !ok || !isRoot {
		r.t.Fatalf("LookupEntry(%q) = root=%v ok=%v err=%v", key, isRoot, ok, err)
	}
	sr, err := decodeStreamRoot(v, nil, nil)
	if err != nil {
		r.t.Fatalf("decode stream root %q: %v", key, err)
	}
	if len(r.model) > 0 && sr.last != r.model[len(r.model)-1].id {
		r.t.Fatalf("root last = %v, model last = %v", sr.last, r.model[len(r.model)-1].id)
	}
	var kbuf [SubkeySize]byte
	fence := sr.fence
	if sr.paged {
		// Rebuild the flat view from the pages, auditing the two-level
		// invariants on the way: each page decodes, sums to its index
		// count, and starts at its index base. The run loop below then
		// audits the stitched fence exactly like the flat shape, which
		// also proves ID order across page boundaries.
		fence = nil
		for pi, pe := range sr.pidx {
			putHashFenceKey(kbuf[:], sr.rooth, pe.segid)
			pv, ok, err := r.tr.Get(ctx, kbuf[:])
			if err != nil || !ok {
				r.t.Fatalf("fence page %d (pageid %d) read: ok=%v err=%v", pi, pe.segid, ok, err)
			}
			ents, sum, err := decodeStreamFencePage(pv, sr.nextSegid, nil)
			if err != nil {
				r.t.Fatalf("fence page %d decode: %v", pi, err)
			}
			if sum != uint64(pe.count) {
				r.t.Fatalf("fence page %d sums to %d, index says %d", pi, sum, pe.count)
			}
			if ents[0].base != pe.base {
				r.t.Fatalf("fence page %d starts at %v, index says %v", pi, ents[0].base, pe.base)
			}
			fence = append(fence, ents...)
		}
	}
	total := uint64(0)
	prev := streamID{}
	mi := 0
	for fi, fe := range fence {
		putHashSegKey(kbuf[:], sr.rooth, fe.segid)
		rv, ok, err := r.tr.Get(ctx, kbuf[:])
		if err != nil || !ok {
			r.t.Fatalf("run %d (segid %d) read: ok=%v err=%v", fi, fe.segid, ok, err)
		}
		var ents []streamEntry
		info, err := walkStreamRun(rv, func(_ int, e streamEntry) error {
			ents = append(ents, streamEntry{id: e.id, fv: append([][]byte{}, e.fv...), dead: e.dead})
			return nil
		})
		if err != nil {
			r.t.Fatalf("run %d walk: %v", fi, err)
		}
		if info.base != fe.base {
			r.t.Fatalf("run %d base %v, fence says %v", fi, info.base, fe.base)
		}
		if info.live != int(fe.count) {
			r.t.Fatalf("run %d live %d, fence says %d", fi, info.live, fe.count)
		}
		if !prev.less(info.base) {
			r.t.Fatalf("run %d base %v not above the previous run's last %v", fi, info.base, prev)
		}
		prev = info.last
		if re := appendStreamRun(nil, ents); !bytes.Equal(re, rv) {
			r.t.Fatalf("run %d: stored %d bytes differ from the from-scratch re-encode (%d bytes)", fi, len(rv), len(re))
		}
		for _, e := range ents {
			if e.dead {
				continue
			}
			if mi >= len(r.model) {
				r.t.Fatalf("run %d holds entry %v past the model's %d entries", fi, e.id, len(r.model))
			}
			m := r.model[mi]
			if e.id != m.id {
				r.t.Fatalf("entry %d id = %v, model says %v", mi, e.id, m.id)
			}
			if len(e.fv) != len(m.fv) {
				r.t.Fatalf("entry %v holds %d field strings, model says %d", e.id, len(e.fv), len(m.fv))
			}
			for f := range e.fv {
				if !bytes.Equal(e.fv[f], m.fv[f]) {
					r.t.Fatalf("entry %v field string %d = %q, model says %q", e.id, f, e.fv[f], m.fv[f])
				}
			}
			mi++
		}
		total += uint64(info.live)
	}
	if total != sr.count {
		r.t.Fatalf("runs hold %d live entries, root count says %d", total, sr.count)
	}
	if mi != len(r.model) {
		r.t.Fatalf("runs hold %d live entries, model holds %d", mi, len(r.model))
	}
	n, err := r.x.Len(ctx, []byte(key))
	if err != nil || n != int64(len(r.model)) {
		r.t.Fatalf("Len = %d err=%v, model holds %d", n, err, len(r.model))
	}
}

// rangeIDs collects a Range call's output, copying the emitted spans
// that die at the next IO round.
func (r *streamRig) rangeIDs(key string, start, end streamID, count int64, rev bool) []streamModelEnt {
	r.t.Helper()
	var out []streamModelEnt
	announced := -1
	err := r.x.Range(context.Background(), []byte(key), start, end, count, rev, func(n int) {
		if announced >= 0 {
			r.t.Fatal("begin ran twice")
		}
		announced = n
	}, func(id streamID, fv [][]byte) {
		cp := make([][]byte, len(fv))
		for i, b := range fv {
			cp[i] = append([]byte(nil), b...)
		}
		out = append(out, streamModelEnt{id: id, fv: cp})
	})
	if err != nil {
		r.t.Fatalf("Range(%q, %v..%v): %v", key, start, end, err)
	}
	if announced != len(out) {
		r.t.Fatalf("begin announced %d entries, %d emitted", announced, len(out))
	}
	return out
}

// modelRange is the oracle: the model's live window, capped and
// possibly reversed.
func (r *streamRig) modelRange(start, end streamID, count int64, rev bool) []streamModelEnt {
	var out []streamModelEnt
	for _, m := range r.model {
		if m.id.less(start) || end.less(m.id) {
			continue
		}
		out = append(out, m)
	}
	if rev {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	if count >= 0 && int64(len(out)) > count {
		out = out[:count]
	}
	return out
}

func (r *streamRig) checkRange(key string, start, end streamID, count int64, rev bool) {
	r.t.Helper()
	got := r.rangeIDs(key, start, end, count, rev)
	want := r.modelRange(start, end, count, rev)
	if len(got) != len(want) {
		r.t.Fatalf("range %v..%v count=%d rev=%v: %d entries, want %d", start, end, count, rev, len(got), len(want))
	}
	for i := range got {
		if got[i].id != want[i].id {
			r.t.Fatalf("range %v..%v rev=%v entry %d = %v, want %v", start, end, rev, i, got[i].id, want[i].id)
		}
		for f := range got[i].fv {
			if !bytes.Equal(got[i].fv[f], want[i].fv[f]) {
				r.t.Fatalf("range entry %v field string %d = %q, want %q", got[i].id, f, got[i].fv[f], want[i].fv[f])
			}
		}
	}
}

// TestStreamAddInvariants drives the auto-ID appends every feed uses:
// same-millisecond bursts riding the fast amendment, clock advances,
// a backwards clock bumping off the last ID, and enough entries to cut
// runs at the entry cap, with the full audit after every single add.
func TestStreamAddInvariants(t *testing.T) {
	r := newStreamRig(t)
	for i := range 300 {
		switch {
		case i%7 == 3:
			r.nowMs += 5
		case i%31 == 17:
			// The clock steps back; the ID must still climb.
			r.nowMs -= 3
		}
		r.add("s", xidAuto, streamID{}, "sensor", fmt.Sprintf("v%d", i), "unit", "C")
	}
}

// TestStreamSchemaDrift exercises every name-table shape: the stable
// table, growth re-encodes, the seventeenth distinct name inlining, a
// long name inlining past the u8 length, and fat values cutting a run
// per entry.
func TestStreamSchemaDrift(t *testing.T) {
	r := newStreamRig(t)
	// Grow the table one name at a time; each new name forces the
	// re-encode amendment.
	for i := range 20 {
		r.nowMs++
		r.add("s", xidAuto, streamID{}, fmt.Sprintf("name%02d", i), "v")
	}
	// The table is full at sixteen: short names past it inline, and so
	// does a name longer than a table slot at any fill.
	r.add("s", xidAuto, streamID{}, "name03", "again", "fresh", "inlined")
	r.add("s", xidAuto, streamID{}, strings.Repeat("n", 300), "long")
	// Fat values overflow the byte cap, so every add cuts a run.
	for range 3 {
		r.nowMs++
		r.add("s", xidAuto, streamID{}, "blob", strings.Repeat("x", 5000))
	}
	// Duplicate names inside one entry are legal and ordered.
	r.add("s", xidAuto, streamID{}, "name00", "1", "name00", "2")
}

// TestStreamExplicitIDs drives the explicit and auto-seq grammars and
// their validation errors, checking the refusals leave no trace.
func TestStreamExplicitIDs(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	add := func(mode int, req streamID) error {
		_, _, err := r.x.Add(ctx, []byte("s"), mode, req, r.nowMs, false, [][]byte{[]byte("f"), []byte("v")})
		return err
	}

	// 0-0 refuses even on a fresh key.
	if err := add(xidExplicit, streamID{}); !errors.Is(err, errXaddZeroID) {
		t.Fatalf("0-0 err = %v", err)
	}
	// ms-* on a fresh key yields ms-0, and 0-* yields 0-1 because the
	// zero ID is not generable.
	if id := r.add("s", xidAutoSeq, streamID{ms: 5}, "f", "v"); id != (streamID{ms: 5}) {
		t.Fatalf("5-* on fresh = %v", id)
	}
	if id := r.add("s", xidAutoSeq, streamID{ms: 5}, "f", "v"); id != (streamID{ms: 5, seq: 1}) {
		t.Fatalf("5-* again = %v", id)
	}
	// Equal or smaller explicit IDs refuse, and the state is untouched
	// (the audit after the next good add would catch a half-write).
	if err := add(xidExplicit, streamID{ms: 5, seq: 1}); !errors.Is(err, errXaddSmallID) {
		t.Fatalf("equal ID err = %v", err)
	}
	if err := add(xidExplicit, streamID{ms: 4, seq: 9}); !errors.Is(err, errXaddSmallID) {
		t.Fatalf("smaller ID err = %v", err)
	}
	if err := add(xidAutoSeq, streamID{ms: 4}); !errors.Is(err, errXaddSmallID) {
		t.Fatalf("4-* below last err = %v", err)
	}
	r.check("s")
	r.add("s", xidExplicit, streamID{ms: 7, seq: 3}, "f", "v")

	// A saturated seq under ms-* answers the too-small error, Redis
	// 8.8's observed reply, and the auto mode near a saturated clock
	// bumps seq off the last ID.
	r.add("s", xidExplicit, streamID{ms: 9, seq: math.MaxUint64}, "f", "v")
	if err := add(xidAutoSeq, streamID{ms: 9}); !errors.Is(err, errXaddSmallID) {
		t.Fatalf("9-* saturated err = %v", err)
	}
	r.nowMs = 9
	if id := r.add("s", xidAuto, streamID{}, "f", "v"); id != (streamID{ms: 10}) {
		t.Fatalf("auto over saturated seq = %v", id)
	}
	r.nowMs = 3
	if id := r.add("s", xidAuto, streamID{}, "f", "v"); id != (streamID{ms: 10, seq: 1}) {
		t.Fatalf("auto behind last = %v", id)
	}

	// The full exhaustion refuses.
	r.add("s", xidExplicit, streamID{ms: math.MaxUint64, seq: math.MaxUint64}, "f", "v")
	if err := add(xidAuto, streamID{}); !errors.Is(err, errXaddExhausted) {
		t.Fatalf("exhausted err = %v", err)
	}
	r.check("s")
}

// TestStreamNoMkStream pins the miss reply shape: no key, no write.
func TestStreamNoMkStream(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	_, ok, err := r.x.Add(ctx, []byte("s"), xidAuto, streamID{}, r.nowMs, true, [][]byte{[]byte("f"), []byte("v")})
	if err != nil || ok {
		t.Fatalf("NOMKSTREAM on missing = ok=%v err=%v", ok, err)
	}
	if n, err := r.x.Len(ctx, []byte("s")); err != nil || n != 0 {
		t.Fatalf("Len after miss = %d err=%v", n, err)
	}
	r.add("s", xidAuto, streamID{}, "f", "v")
	id, ok, err := r.x.Add(ctx, []byte("s"), xidAuto, streamID{}, r.nowMs, true, [][]byte{[]byte("g"), []byte("w")})
	if err != nil || !ok {
		t.Fatalf("NOMKSTREAM on live = ok=%v err=%v", ok, err)
	}
	r.model = append(r.model, streamModelEnt{id: id, fv: [][]byte{[]byte("g"), []byte("w")}})
	r.check("s")
}

// TestStreamRangeModel compares Range against the model over windows
// that start and end mid-run, cap by count, and walk both directions,
// with enough runs to force multiple prefetch rounds.
func TestStreamRangeModel(t *testing.T) {
	r := newStreamRig(t)
	// 300 small entries cut at the entry cap; explicit IDs make the
	// window arithmetic legible.
	for i := 1; i <= 300; i++ {
		fv := [][]byte{[]byte("f"), []byte(fmt.Sprintf("v%d", i))}
		if _, _, err := r.x.Add(context.Background(), []byte("s"), xidExplicit, streamID{ms: uint64(i), seq: 1}, r.nowMs, false, fv); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
		r.model = append(r.model, streamModelEnt{id: streamID{ms: uint64(i), seq: 1}, fv: fv})
	}
	r.check("s")
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	for _, rev := range []bool{false, true} {
		r.checkRange("s", streamID{}, full, -1, rev)
		r.checkRange("s", streamID{ms: 100}, streamID{ms: 250, seq: math.MaxUint64}, -1, rev)
		r.checkRange("s", streamID{ms: 100, seq: 2}, streamID{ms: 250}, -1, rev)
		r.checkRange("s", streamID{}, full, 7, rev)
		r.checkRange("s", streamID{ms: 128, seq: 1}, streamID{ms: 129, seq: 1}, -1, rev)
		r.checkRange("s", streamID{ms: 500}, full, -1, rev)
		r.checkRange("s", streamID{ms: 42}, streamID{ms: 42, seq: math.MaxUint64}, -1, rev)
	}

	// Fat entries: one run each, more runs than one prefetch round.
	r2 := newStreamRig(t)
	fat := strings.Repeat("y", 5000)
	for i := 1; i <= 20; i++ {
		r2.add("f", xidExplicit, streamID{ms: uint64(i), seq: 0}, "blob", fat, "n", fmt.Sprintf("%d", i))
	}
	for _, rev := range []bool{false, true} {
		r2.checkRange("f", streamID{}, full, -1, rev)
		r2.checkRange("f", streamID{ms: 3}, streamID{ms: 17, seq: math.MaxUint64}, 9, rev)
	}

	// A missing key announces zero.
	r2.checkRangeMissing("nosuch")
}

func (r *streamRig) checkRangeMissing(key string) {
	r.t.Helper()
	saved := r.model
	r.model = nil
	r.checkRange(key, streamID{}, streamID{ms: math.MaxUint64, seq: math.MaxUint64}, -1, false)
	r.model = saved
}

// TestStreamWrongType pins the cross-type doors at the layer.
func TestStreamWrongType(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	if err := r.tr.Set(ctx, []byte("str"), []byte("v"), TagString); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.x.Add(ctx, []byte("str"), xidAuto, streamID{}, r.nowMs, false, [][]byte{[]byte("f"), []byte("v")}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Add on string err = %v", err)
	}
	if _, err := r.x.Len(ctx, []byte("str")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Len on string err = %v", err)
	}
	err := r.x.Range(ctx, []byte("str"), streamID{}, streamID{ms: 1}, -1, false, func(int) {}, func(streamID, [][]byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("Range on string err = %v", err)
	}
}

// TestStreamReopen proves the cold view: a fresh runtime over the same
// store reads the identical stream after a flush.
func TestStreamReopen(t *testing.T) {
	r := newStreamRig(t)
	for i := range 150 {
		if i%5 == 0 {
			r.nowMs++
		}
		r.add("s", xidAuto, streamID{}, "k", fmt.Sprintf("v%d", i))
	}
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	tr2 := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return r.nowMs },
	})
	x2, err := NewStream(tr2, StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cold := &streamRig{t: t, rs: r.rs, tr: tr2, x: x2, nowMs: r.nowMs, model: r.model}
	cold.check("s")
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	cold.checkRange("s", streamID{}, full, -1, false)
	cold.checkRange("s", streamID{}, full, -1, true)
}
