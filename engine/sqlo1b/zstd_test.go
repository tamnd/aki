package sqlo1b

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// zstdJSONValue builds one distinct json-like value: internal
// redundancy zstd eats, but every value differs so the dictionary
// schemes fail the floor and the values are not integer-shaped, which
// is exactly the fall-through shape from the cascade lab.
func zstdJSONValue(i, size int) []byte {
	v := fmt.Appendf(nil, `{"id":"user-%08d","status":"active","note":"`, i)
	for len(v) < size-2 {
		v = fmt.Appendf(v, "event %d ok;", i%7)
	}
	return append(v[:size-2], '"', '}')
}

func zstdShape(t testing.TB) []byte {
	t.Helper()
	values := make([][]byte, 60)
	for i := range values {
		values[i] = zstdJSONValue(i, 120)
	}
	return cascadePayload(t, values)
}

// TestZstdSelectFallThrough pins the selection order: the json shape
// falls past every lightweight scheme to zstd, the lab shapes the
// cascade owns never reach it, and the winner round-trips.
func TestZstdSelectFallThrough(t *testing.T) {
	payload := zstdShape(t)
	scheme, comp := cSelect(payload)
	if scheme != SchemeZstd {
		t.Fatalf("json shape selected scheme %d, want zstd", scheme)
	}
	if 100*len(comp) > (100-cSelectFloor)*len(payload) {
		t.Fatalf("zstd winner of %d bytes is under the floor on %d raw", len(comp), len(payload))
	}
	got, err := cDecode(SchemeZstd, 0, comp, len(payload))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("zstd winner does not round trip: %v", err)
	}
	for name, want := range map[string][]uint8{
		"lowcard":  {SchemeDict, SchemeDictRLE},
		"counters": {SchemeFor},
	} {
		if scheme, _ := cSelect(cascadeShapes(t)[name]); scheme != want[0] && (len(want) < 2 || scheme != want[1]) {
			t.Errorf("%s selected scheme %d with zstd registered, want one of %v", name, scheme, want)
		}
	}
}

// TestZstdDecodeRejections drives the scheme 5 decode through the
// corrupt shapes it must refuse.
func TestZstdDecodeRejections(t *testing.T) {
	payload := zstdShape(t)
	comp := zstdEncode(payload)
	if _, err := cDecode(SchemeZstd, 0, comp, len(payload)); err != nil {
		t.Fatal(err)
	}
	reject := func(name string, comp []byte, ulen int) {
		if _, err := cDecode(SchemeZstd, 0, comp, ulen); err == nil {
			t.Errorf("%s decoded", name)
		}
	}
	reject("ulen one short", comp, len(payload)-1)
	reject("ulen one long", comp, len(payload)+1)
	reject("ulen negative", comp, -1)
	reject("ulen past the frame bound", comp, cframeMaxUlen+1)
	reject("empty frame with ulen", nil, len(payload))
	reject("garbage bytes", []byte("not a zstd frame at all"), len(payload))
	trunc := bytes.Clone(comp[:len(comp)/2])
	reject("truncated frame", trunc, len(payload))
	flip := bytes.Clone(comp)
	flip[len(flip)/2] ^= 0x40
	reject("flipped payload bit", flip, len(payload))
	if _, err := cDecode(SchemeZstd, 3, comp, len(payload)); err == nil {
		t.Error("plain zstd frame naming a dictionary decoded")
	}
}

// TestZstdSealFrame seals a builder full of json values: the frame
// stamps scheme 5, shrinks, and parses back to byte-exact records.
func TestZstdSealFrame(t *testing.T) {
	g := NewCGroupBuilder(GroupSize)
	var recs [][]byte
	for i := 0; ; i++ {
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "js-%03d", i), Value: zstdJSONValue(i, 300)}
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
	img := g.Seal()
	if g.Scheme() != SchemeZstd {
		t.Fatalf("sealed json values as scheme %d, want zstd", g.Scheme())
	}
	if clen := int(binary.LittleEndian.Uint32(img[8:])); clen >= g.used {
		t.Fatalf("sealed clen %d did not shrink %d payload bytes", clen, g.used)
	}
	v, err := ParseCGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if v.Scheme() != SchemeZstd || v.Records() != len(recs) {
		t.Fatalf("sealed image parses as scheme %d with %d records", v.Scheme(), v.Records())
	}
	for i, want := range recs {
		got, err := v.Record(uint16(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("sealed record %d diverged: %v", i, err)
		}
	}
}

// TestCompactZstdStats runs the store end to end on the fall-through
// shape: distinct compressible values compact into zstd groups, the
// histogram books scheme 5, and reads verify across checkpoint and
// reopen.
func TestCompactZstdStats(t *testing.T) {
	r := newStoreRig(t)
	r.apply(t, putOp("zs-seed", []byte("x"), 0))
	first := r.s.vlog.ext
	n := 0
	for batch := 0; r.s.vlog.ext == first; batch++ {
		if batch >= 400 {
			t.Fatalf("vlog extent never rolled after %d batches", batch)
		}
		ops := make([]sqlo1.Op, 0, 8)
		for range 8 {
			ops = append(ops, putOp(fmt.Sprintf("zs-fill%05d", n), zstdJSONValue(n, 950), 0))
			n++
		}
		r.apply(t, ops...)
	}
	if _, err := r.s.CompactExtent(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	sg := r.s.SchemeGroups()
	if sg[SchemeZstd] == 0 {
		t.Fatalf("no zstd groups booked in the scheme histogram: %v", sg)
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.verify(t)
	r.reopen(t)
	r.verify(t)
}

func FuzzZstdDecode(f *testing.F) {
	payload := zstdShape(f)
	f.Add(zstdEncode(payload), len(payload))
	f.Add(zstdEncode(nil), 0)
	f.Add([]byte{0x28, 0xb5, 0x2f, 0xfd}, 100)
	f.Fuzz(func(t *testing.T, comp []byte, ulen int) {
		out, err := cDecode(SchemeZstd, 0, comp, ulen&0xffff)
		if err == nil && len(out) != ulen&0xffff {
			t.Fatalf("decode returned %d bytes for ulen %d", len(out), ulen&0xffff)
		}
	})
}

// TestRecompactReselects proves gen-C recompression re-runs the
// sampled selector: groups mixing json and constant values seal as
// dictionary frames (the constant half clears the floor before the
// fall-through fires), then every constant record dies and the
// recompaction pass re-seals the all-json survivors as zstd. The walk
// relocates raw record bytes through the same closeCompactGroup seal
// as the first pass, so selection tracks the live shape, not the
// original one.
func TestRecompactReselects(t *testing.T) {
	r := newStoreRig(t)
	fillMixed := func(prefix string) uint64 {
		r.apply(t, putOp(prefix+"seed", []byte("x"), 0))
		first := r.s.vlog.ext
		n := 0
		for batch := 0; r.s.vlog.ext == first; batch++ {
			if batch >= 400 {
				t.Fatalf("vlog extent never rolled after %d batches", batch)
			}
			ops := make([]sqlo1.Op, 0, 8)
			for range 8 {
				k := fmt.Sprintf("%smix%05d", prefix, n)
				v := bytes.Repeat([]byte{'c'}, 950)
				if n%2 == 0 {
					v = zstdJSONValue(n, 950)
				}
				ops = append(ops, putOp(k, v, 0))
				n++
			}
			r.apply(t, ops...)
		}
		return first
	}
	ext := fillMixed("a-")
	if _, err := r.s.CompactExtent(context.Background(), ext); err != nil {
		t.Fatal(err)
	}
	first := r.s.cvlog.ext
	// Speculative fits pack the mixed groups well past the raw
	// projection, so the compact extent takes many more fills to roll.
	for i := 1; r.s.cvlog.ext == first; i++ {
		if i > 40 {
			t.Fatalf("compact stream never rolled off extent %d", first)
		}
		if _, err := r.s.CompactExtent(context.Background(), fillMixed(fmt.Sprintf("b%d-", i))); err != nil {
			t.Fatal(err)
		}
	}
	sg1 := r.s.SchemeGroups()
	if sg1[SchemeDict]+sg1[SchemeDictRLE] == 0 {
		t.Fatalf("mixed groups booked no dictionary frames on the first pass: %v", sg1)
	}

	// Kill every constant record inside the sealed frame extent so
	// its survivors are all distinct json, the fall-through shape.
	var dels []sqlo1.Op
	kept := 0
	for k, rec := range r.sh {
		if r.posOf(t, k).Extent() != first {
			continue
		}
		if !bytes.HasPrefix(rec.Value, []byte(`{"id":"user-`)) {
			dels = append(dels, delOp(k))
		} else {
			kept++
		}
	}
	if len(dels) < 5 || kept < 5 {
		t.Fatalf("frame extent holds %d json and %d constant records", len(dels), kept)
	}
	r.apply(t, dels...)
	// Quarantined raw extents settle before the recompaction pass.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cs, err := r.s.CompactExtent(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Relocated == 0 {
		t.Fatalf("recompaction stats %+v", cs)
	}
	sg2 := r.s.SchemeGroups()
	if sg2[SchemeZstd] <= sg1[SchemeZstd] {
		t.Fatalf("json survivors did not reselect zstd: %v -> %v", sg1, sg2)
	}
	r.verify(t)
	// Packing keeps the survivors' compact extent open far longer, so
	// fill until it seals and the scrubber has zstd frames to decode.
	second := r.s.cvlog.ext
	for i := 0; r.s.cvlog.ext == second; i++ {
		if i > 40 {
			t.Fatalf("survivor extent %d never sealed under more fills", second)
		}
		if _, err := r.s.CompactExtent(context.Background(), fillMixed(fmt.Sprintf("c%d-", i))); err != nil {
			t.Fatal(err)
		}
	}
	sc := &Scrubber{File: r.s.f, ExtentSize: r.s.sb.ExtentSize, Grid: r.s.grid}
	if rep := sc.Sweep(); !rep.Clean() || rep.Scanned == 0 {
		t.Fatalf("scrub after reselection (scanned %d): %+v", rep.Scanned, rep.Findings)
	}
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}
