package sqlo1

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

// testStreamRoot is a healthy three-run root the corruption table
// mutates: counts 3+2+4, added past count as trims would leave it.
func testStreamRoot() streamRoot {
	return streamRoot{
		rootgen:   7,
		rooth:     0xabcd1234,
		count:     9,
		added:     12,
		last:      streamID{ms: 40, seq: 5},
		maxDel:    streamID{ms: 12, seq: 0},
		nextSegid: 6,
		fence: []streamFenceEnt{
			{base: streamID{ms: 10, seq: 1}, segid: 0, count: 3},
			{base: streamID{ms: 10, seq: 9}, segid: 3, count: 2},
			{base: streamID{ms: 40, seq: 0}, segid: 5, count: 4},
		},
	}
}

// testStreamRootPaged is the paged mirror: two fence pages holding the
// same nine entries, pageids minted from the shared segid counter.
func testStreamRootPaged() streamRoot {
	return streamRoot{
		paged:     true,
		rootgen:   7,
		rooth:     0xabcd1234,
		count:     9,
		added:     12,
		last:      streamID{ms: 40, seq: 5},
		nextSegid: 8,
		pidx: []streamFenceEnt{
			{base: streamID{ms: 10, seq: 1}, segid: 6, count: 5},
			{base: streamID{ms: 40, seq: 0}, segid: 7, count: 4},
		},
	}
}

func TestStreamRootRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		r    streamRoot
	}{
		{"three runs", testStreamRoot()},
		{"paged", testStreamRootPaged()},
		{"single run", streamRoot{
			rootgen: 1, rooth: 5, count: 1, added: 1,
			last: streamID{ms: 1, seq: 1}, nextSegid: 1,
			fence: []streamFenceEnt{{base: streamID{ms: 1, seq: 1}, segid: 0, count: 1}},
		}},
		// A trimmed-empty stream is legal: no runs, count 0, but the
		// last generated ID survives, unlike a list root.
		{"trimmed empty", streamRoot{
			rootgen: 3, rooth: 9, count: 0, added: 17,
			last: streamID{ms: 99, seq: 4}, maxDel: streamID{ms: 99, seq: 4}, nextSegid: 8,
		}},
		{"extremes", streamRoot{
			rootgen: math.MaxUint32, rooth: math.MaxUint64, count: 1, added: math.MaxUint64,
			last: streamID{ms: math.MaxUint64, seq: math.MaxUint64}, nextSegid: 1 << 47, groupCount: 3,
			fence: []streamFenceEnt{{base: streamID{ms: math.MaxUint64, seq: math.MaxUint64}, segid: 1<<47 - 1, meta: 0xbeef, count: 1}},
		}},
	}
	for _, tc := range cases {
		v := appendStreamRoot(nil, &tc.r)
		got, err := decodeStreamRoot(v, nil, nil)
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if got.paged != tc.r.paged || got.rootgen != tc.r.rootgen || got.rooth != tc.r.rooth || got.count != tc.r.count ||
			got.added != tc.r.added || got.last != tc.r.last || got.maxDel != tc.r.maxDel ||
			got.nextSegid != tc.r.nextSegid || got.groupCount != tc.r.groupCount {
			t.Fatalf("%s: header roundtrip: got %+v want %+v", tc.name, got, tc.r)
		}
		gotEnts, wantEnts := got.fence, tc.r.fence
		if tc.r.paged {
			gotEnts, wantEnts = got.pidx, tc.r.pidx
			if got.fence != nil {
				t.Fatalf("%s: paged root decoded a flat fence", tc.name)
			}
		}
		if len(gotEnts) != len(wantEnts) {
			t.Fatalf("%s: fence length %d, want %d", tc.name, len(gotEnts), len(wantEnts))
		}
		for i := range gotEnts {
			if gotEnts[i] != wantEnts[i] {
				t.Fatalf("%s: fence[%d] = %+v, want %+v", tc.name, i, gotEnts[i], wantEnts[i])
			}
		}
		// The re-encode is byte-identical, the codec discipline every
		// sqlo1 root follows.
		if re := appendStreamRoot(nil, &got); string(re) != string(v) {
			t.Fatalf("%s: re-encode differs", tc.name)
		}
	}
}

func TestStreamRootCorruption(t *testing.T) {
	fe := func(i int) int { return streamRootHdrLen + i*streamFenceEntLen }
	cases := []struct {
		name  string
		paged bool
		mut   func(v []byte) []byte
		want  string
	}{
		{"truncated header", false, func(v []byte) []byte { return v[:streamRootHdrLen-1] }, "has no header"},
		{"wrong sub", false, func(v []byte) []byte { v[0] = ropeSub; return v }, "has sub"},
		{"unknown xflags", false, func(v []byte) []byte { v[1] = 2; return v }, "unknown xflags"},
		{"reserved bytes", false, func(v []byte) []byte { v[2] = 1; return v }, "nonzero reserved"},
		{"rootgen zero", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[4:], 0)
			return v
		}, "rootgen 0"},
		{"count past added", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[24:], 8)
			return v
		}, "exceeds entries added"},
		{"fence count over cap", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[76:], uint32(streamFenceMaxRuns+1))
			return v
		}, "out of range"},
		{"torn fence entry", false, func(v []byte) []byte { return v[:len(v)-1] }, "does not fit"},
		{"trailing bytes", false, func(v []byte) []byte { return append(v, 0) }, "does not fit"},
		{"zero base", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(0):], 0)
			binary.LittleEndian.PutUint64(v[fe(0)+8:], 0)
			return v
		}, "zero base ID"},
		{"base order", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(1):], 9)
			return v
		}, "out of ID order"},
		{"segid past next", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(2)+16:], 6)
			return v
		}, "at or past next_segid"},
		{"entry count zero", false, func(v []byte) []byte {
			// Keep the sum right by moving entry 1's count onto entry 0.
			binary.LittleEndian.PutUint32(v[fe(0)+24:], 5)
			binary.LittleEndian.PutUint32(v[fe(1)+24:], 0)
			return v
		}, "count 0"},
		{"count sum drift", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[fe(1)+24:], 3)
			return v
		}, "sum to"},
		{"last below tail base", false, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[32:], 39)
			return v
		}, "below the tail run"},
		// The paged mirror shares decodeStreamFenceEnts, so one case per
		// paged-specific door suffices: the index bound, the index order,
		// a pageid past the mint, and the index sum.
		{"page index over cap", true, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[76:], uint32(streamFencePageIdxMax+1))
			return v
		}, "page index count"},
		{"page index order", true, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(1):], 9)
			return v
		}, "out of ID order"},
		{"pageid past next", true, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(1)+16:], 8)
			return v
		}, "at or past next_segid"},
		{"page index sum drift", true, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[fe(1)+24:], 3)
			return v
		}, "sum to"},
	}
	for _, tc := range cases {
		r := testStreamRoot()
		if tc.paged {
			r = testStreamRootPaged()
		}
		v := tc.mut(appendStreamRoot(nil, &r))
		if _, err := decodeStreamRoot(v, nil, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

// TestStreamFencePageCodec pins the page payload codec: roundtrip at a
// small and a full fanout, then the corruption doors, with the entry
// validation shared with the root's arrays.
func TestStreamFencePageCodec(t *testing.T) {
	ents := []streamFenceEnt{
		{base: streamID{ms: 10, seq: 1}, segid: 0, count: 3},
		{base: streamID{ms: 10, seq: 9}, segid: 3, count: 2},
		{base: streamID{ms: 40, seq: 0}, segid: 5, count: 4},
	}
	full := make([]streamFenceEnt, streamFencePageMax)
	for i := range full {
		full[i] = streamFenceEnt{base: streamID{ms: uint64(i + 1)}, segid: uint64(i), count: 1}
	}
	for _, tc := range []struct {
		name      string
		ents      []streamFenceEnt
		nextSegid uint64
	}{
		{"three entries", ents, 6},
		{"full page", full, uint64(streamFencePageMax)},
	} {
		v := appendStreamFencePage(nil, tc.ents)
		got, sum, err := decodeStreamFencePage(v, tc.nextSegid, nil)
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if sum != streamFenceElems(tc.ents) {
			t.Fatalf("%s: sum = %d, want %d", tc.name, sum, streamFenceElems(tc.ents))
		}
		if len(got) != len(tc.ents) {
			t.Fatalf("%s: %d entries, want %d", tc.name, len(got), len(tc.ents))
		}
		for i := range got {
			if got[i] != tc.ents[i] {
				t.Fatalf("%s: entry %d = %+v, want %+v", tc.name, i, got[i], tc.ents[i])
			}
		}
		if re := appendStreamFencePage(nil, got); string(re) != string(v) {
			t.Fatalf("%s: re-encode differs", tc.name)
		}
	}

	pe := func(i int) int { return streamPageHdrLen + i*streamFenceEntLen }
	cases := []struct {
		name      string
		nextSegid uint64
		mut       func(v []byte) []byte
		want      string
	}{
		{"truncated header", 6, func(v []byte) []byte { return v[:streamPageHdrLen-1] }, "has no header"},
		{"count zero", 6, func(v []byte) []byte {
			binary.LittleEndian.PutUint16(v, 0)
			return v
		}, "out of range"},
		{"count over max", 6, func(v []byte) []byte {
			binary.LittleEndian.PutUint16(v, uint16(streamFencePageMax+1))
			return v
		}, "out of range"},
		{"reserved bytes", 6, func(v []byte) []byte { v[2] = 1; return v }, "nonzero reserved"},
		{"torn entry", 6, func(v []byte) []byte { return v[:len(v)-1] }, "does not fit"},
		{"trailing bytes", 6, func(v []byte) []byte { return append(v, 0) }, "does not fit"},
		{"zero base", 6, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[pe(0):], 0)
			binary.LittleEndian.PutUint64(v[pe(0)+8:], 0)
			return v
		}, "zero base ID"},
		{"base order", 6, func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[pe(1):], 9)
			return v
		}, "out of ID order"},
		{"segid past next", 4, func(v []byte) []byte { return v }, "at or past next_segid"},
		{"entry count zero", 6, func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[pe(1)+24:], 0)
			return v
		}, "count 0"},
	}
	for _, tc := range cases {
		v := tc.mut(appendStreamFencePage(nil, ents))
		if _, _, err := decodeStreamFencePage(v, tc.nextSegid, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}
