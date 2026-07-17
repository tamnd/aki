package sqlo1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
)

// srEnt builds an entry from string pairs.
func srEnt(ms, seq uint64, dead bool, pairs ...string) streamEntry {
	if len(pairs)%2 != 0 {
		panic("srEnt pairs")
	}
	fv := make([][]byte, len(pairs))
	for i, s := range pairs {
		fv[i] = []byte(s)
	}
	return streamEntry{id: streamID{ms: ms, seq: seq}, fv: fv, dead: dead}
}

// srWalkAll walks a payload and deep-copies every entry out of the
// walker's aliased scratch.
func srWalkAll(t *testing.T, v []byte) ([]streamEntry, streamRunInfo) {
	t.Helper()
	var out []streamEntry
	info, err := walkStreamRun(v, func(i int, e streamEntry) error {
		fv := make([][]byte, len(e.fv))
		for j, b := range e.fv {
			fv[j] = append([]byte(nil), b...)
		}
		out = append(out, streamEntry{id: e.id, fv: fv, dead: e.dead})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return out, info
}

func srEqual(t *testing.T, tag string, got, want []streamEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: walked %d entries, want %d", tag, len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.id != w.id || g.dead != w.dead {
			t.Fatalf("%s: entry %d is %d-%d dead=%v, want %d-%d dead=%v", tag, i, g.id.ms, g.id.seq, g.dead, w.id.ms, w.id.seq, w.dead)
		}
		if len(g.fv) != len(w.fv) {
			t.Fatalf("%s: entry %d has %d field halves, want %d", tag, i, len(g.fv), len(w.fv))
		}
		for j := range w.fv {
			if !bytes.Equal(g.fv[j], w.fv[j]) {
				t.Fatalf("%s: entry %d field half %d is %q, want %q", tag, i, j, g.fv[j], w.fv[j])
			}
		}
	}
}

// TestStreamRunRoundtrip drives random entry shapes through encode,
// walk, and re-encode: bursty and sparse IDs, name pools crossing the
// table cap, long names past the u8 bound, duplicate names inside one
// entry, empty names and values, fat values, and tomb marks. The
// re-encode must reproduce the payload byte for byte, the canonical
// property the whole codec leans on.
func TestStreamRunRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(29))
	long := strings.Repeat("L", 300)
	pool := []string{"", long}
	for i := range 20 {
		pool = append(pool, fmt.Sprintf("field%d", i))
	}
	for iter := range 300 {
		nameCut := 2 + rng.Intn(len(pool)-1)
		n := 1 + rng.Intn(streamRunMaxEntries)
		id := streamID{ms: uint64(rng.Int63n(1 << 40)), seq: uint64(rng.Intn(3))}
		entries := make([]streamEntry, 0, n)
		for range n {
			nf := rng.Intn(6)
			var pairs []string
			for range nf {
				pairs = append(pairs, pool[rng.Intn(nameCut)])
				vl := rng.Intn(60)
				if rng.Intn(50) == 0 {
					vl = 5000
				}
				pairs = append(pairs, strings.Repeat("v", vl))
			}
			e := srEnt(id.ms, id.seq, rng.Intn(10) == 0, pairs...)
			entries = append(entries, e)
			if rng.Intn(3) == 0 {
				id.ms += 1 + uint64(rng.Int63n(1000))
				id.seq = uint64(rng.Intn(2))
			} else {
				id.seq += 1 + uint64(rng.Intn(4))
			}
		}
		v := appendStreamRun(nil, entries)
		got, info := srWalkAll(t, v)
		srEqual(t, fmt.Sprintf("iter %d", iter), got, entries)
		deads := 0
		for i := range entries {
			if entries[i].dead {
				deads++
			}
		}
		if info.base != entries[0].id || info.last != entries[n-1].id {
			t.Fatalf("iter %d: info span %v..%v, want %v..%v", iter, info.base, info.last, entries[0].id, entries[n-1].id)
		}
		if info.n != n || info.live != n-deads || info.tombs != (deads > 0) {
			t.Fatalf("iter %d: info counts n=%d live=%d tombs=%v, want n=%d live=%d tombs=%v", iter, info.n, info.live, info.tombs, n, n-deads, deads > 0)
		}
		re := appendStreamRun(nil, got)
		if !bytes.Equal(re, v) {
			t.Fatalf("iter %d: re-encode differs, %d vs %d bytes", iter, len(re), len(v))
		}
	}
}

// TestStreamRunFieldOrder pins the Redis-parity ordering rules: fields
// come back in XADD argument order, duplicates included, and the name
// table caps at sixteen with the seventeenth distinct name inlined and
// still order-exact.
func TestStreamRunFieldOrder(t *testing.T) {
	dup := []streamEntry{srEnt(9, 0, false, "b", "1", "a", "2", "b", "3")}
	got, _ := srWalkAll(t, appendStreamRun(nil, dup))
	srEqual(t, "dup", got, dup)

	var pairs []string
	for i := range 20 {
		pairs = append(pairs, fmt.Sprintf("n%02d", i), fmt.Sprintf("v%d", i))
	}
	wide := []streamEntry{srEnt(9, 0, false, pairs...), srEnt(9, 1, false, pairs...)}
	v := appendStreamRun(nil, wide)
	if v[18] != streamNameTableMax {
		t.Fatalf("wide run has name table size %d, want %d", v[18], streamNameTableMax)
	}
	got, _ = srWalkAll(t, v)
	srEqual(t, "wide", got, wide)
}

func TestStreamUvarintLen(t *testing.T) {
	for _, x := range []uint64{0, 1, 127, 128, 1 << 14, 1<<14 - 1, 1 << 35, 1 << 63, math.MaxUint64} {
		if got, want := streamUvarintLen(x), len(binary.AppendUvarint(nil, x)); got != want {
			t.Fatalf("uvarint len of %d is %d, want %d", x, got, want)
		}
	}
}

// srb hand-assembles run payloads for the corruption table.
type srb struct{ b []byte }

func (r *srb) hdr(ms, seq uint64, n, nnames int, tflags uint16) *srb {
	var h [streamRunHdrLen]byte
	binary.LittleEndian.PutUint64(h[0:], ms)
	binary.LittleEndian.PutUint64(h[8:], seq)
	binary.LittleEndian.PutUint16(h[16:], uint16(n))
	h[18] = uint8(nnames)
	binary.LittleEndian.PutUint16(h[19:], tflags)
	r.b = append(r.b, h[:]...)
	return r
}

func (r *srb) name(s string) *srb {
	r.b = append(r.b, uint8(len(s)))
	r.b = append(r.b, s...)
	return r
}

func (r *srb) uv(xs ...uint64) *srb {
	for _, x := range xs {
		r.b = binary.AppendUvarint(r.b, x)
	}
	return r
}

func (r *srb) raw(bs ...byte) *srb {
	r.b = append(r.b, bs...)
	return r
}

// TestStreamRunDecodeErrors holds every validation clause to a named
// rejection, the hand-built forms covering what no encoder output can
// reach: non-minimal varints, table order violations, covered inline
// names, and tomb bitmap shapes.
func TestStreamRunDecodeErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    []byte
		want string
	}{
		{"short header", make([]byte, 5), "has no header"},
		{"zero base", new(srb).hdr(0, 0, 1, 0, 0).uv(0, 0, 0).b, "zero ID"},
		{"count zero", new(srb).hdr(7, 0, 0, 0, 0).b, "entry count 0 out of range"},
		{"count over", new(srb).hdr(7, 0, streamRunMaxEntries+1, 0, 0).b, "entry count 129 out of range"},
		{"table over", new(srb).hdr(7, 0, 1, streamNameTableMax+1, 0).b, "name table size 17 out of range"},
		{"unknown tflags", new(srb).hdr(7, 0, 1, 0, 2).uv(0, 0, 0).b, "unknown tflags"},
		{"torn tomb", new(srb).hdr(7, 0, 1, 0, sflagTombs).b, "torn at the tomb bitmap"},
		{"tomb padding", new(srb).hdr(7, 0, 1, 0, sflagTombs).uv(0, 0, 0).raw(0x02).b, "padding bits set"},
		{"tomb no bits", new(srb).hdr(7, 0, 1, 0, sflagTombs).uv(0, 0, 0).raw(0x00).b, "no bits set"},
		{"torn table", new(srb).hdr(7, 0, 1, 1, 0).raw(250, 'x').b, "name table entry 0 torn"},
		{"dup table name", new(srb).hdr(7, 0, 1, 2, 0).name("a").name("a").uv(0, 0, 2).raw(0).uv(0).raw(1).uv(0).b, "duplicates an earlier name"},
		{"first entry deltas", new(srb).hdr(7, 0, 1, 0, 0).uv(1, 0, 0).b, "nonzero deltas against the base"},
		{"repeat ID", new(srb).hdr(7, 0, 2, 0, 0).uv(0, 0, 0).uv(0, 0, 0).b, "repeats the previous ID"},
		{"seq overflow", new(srb).hdr(7, math.MaxUint64, 2, 0, 0).uv(0, 0, 0).uv(0, 1, 0).b, "overflows seq"},
		{"ms overflow", new(srb).hdr(math.MaxUint64, 0, 2, 0, 0).uv(0, 0, 0).uv(1, 0, 0).b, "overflows ms"},
		{"non-minimal varint", new(srb).hdr(7, 0, 1, 0, 0).raw(0x80, 0x00).uv(0, 0).b, "non-minimal dms varint"},
		{"torn entry", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0).b, "torn at nfields"},
		{"torn field", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 1).b, "torn at field 0"},
		{"ref past table", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 1).raw(0).b, "references name 0 past the table"},
		{"ref order", new(srb).hdr(7, 0, 1, 2, 0).name("a").name("b").uv(0, 0, 2).raw(1).uv(0).raw(0).uv(0).b, "not in first-reference order"},
		{"unreferenced name", new(srb).hdr(7, 0, 1, 2, 0).name("a").name("b").uv(0, 0, 1).raw(0).uv(0).b, "only 1 are referenced"},
		{"covered inline", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 1).raw(streamNameInline).uv(1).raw('x').uv(0).b, "inlines a name the table covers"},
		{"torn inline name", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 1).raw(streamNameInline).uv(50).raw('x', 'y').b, "name torn at"},
		{"torn value", new(srb).hdr(7, 0, 1, 1, 0).name("a").uv(0, 0, 1).raw(0).uv(100).raw('x').b, "value torn at"},
		{"trailing bytes", new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 0).raw(0).b, "trailing bytes"},
	} {
		_, err := walkStreamRun(tc.v, nil)
		if err == nil {
			t.Fatalf("%s: decoded", tc.name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: error %q does not name %q", tc.name, err, tc.want)
		}
	}
}

// FuzzStreamRun is the codec fuzz: no input may panic the walker, and
// any input it accepts must re-encode byte-identically, which is the
// canonical-form contract stated on walkStreamRun.
func FuzzStreamRun(f *testing.F) {
	f.Add(appendStreamRun(nil, []streamEntry{srEnt(7, 0, false, "a", "1", "b", "2"), srEnt(7, 1, true, "a", "3")}))
	var pairs []string
	for i := range 18 {
		pairs = append(pairs, fmt.Sprintf("n%02d", i), "v")
	}
	f.Add(appendStreamRun(nil, []streamEntry{srEnt(1, 5, false, pairs...), srEnt(900, 0, false, strings.Repeat("N", 300), "")}))
	f.Add(new(srb).hdr(7, 0, 1, 0, 0).uv(0, 0, 0).b)
	f.Add([]byte("stream"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var out []streamEntry
		info, err := walkStreamRun(data, func(i int, e streamEntry) error {
			fv := make([][]byte, len(e.fv))
			for j, b := range e.fv {
				fv[j] = append([]byte(nil), b...)
			}
			out = append(out, streamEntry{id: e.id, fv: fv, dead: e.dead})
			return nil
		})
		if err != nil {
			return
		}
		if len(out) != info.n {
			t.Fatalf("walked %d entries, info says %d", len(out), info.n)
		}
		if re := appendStreamRun(nil, out); !bytes.Equal(re, data) {
			t.Fatalf("accepted payload re-encodes differently, %d vs %d bytes", len(re), len(data))
		}
	})
}
