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

func TestStreamRootRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		r    streamRoot
	}{
		{"three runs", testStreamRoot()},
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
		got, err := decodeStreamRoot(v, nil)
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if got.rootgen != tc.r.rootgen || got.rooth != tc.r.rooth || got.count != tc.r.count ||
			got.added != tc.r.added || got.last != tc.r.last || got.maxDel != tc.r.maxDel ||
			got.nextSegid != tc.r.nextSegid || got.groupCount != tc.r.groupCount {
			t.Fatalf("%s: header roundtrip: got %+v want %+v", tc.name, got, tc.r)
		}
		if len(got.fence) != len(tc.r.fence) {
			t.Fatalf("%s: fence length %d, want %d", tc.name, len(got.fence), len(tc.r.fence))
		}
		for i := range got.fence {
			if got.fence[i] != tc.r.fence[i] {
				t.Fatalf("%s: fence[%d] = %+v, want %+v", tc.name, i, got.fence[i], tc.r.fence[i])
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
		name string
		mut  func(v []byte) []byte
		want string
	}{
		{"truncated header", func(v []byte) []byte { return v[:streamRootHdrLen-1] }, "has no header"},
		{"wrong sub", func(v []byte) []byte { v[0] = ropeSub; return v }, "has sub"},
		{"unknown xflags", func(v []byte) []byte { v[1] = 1; return v }, "unknown xflags"},
		{"reserved bytes", func(v []byte) []byte { v[2] = 1; return v }, "nonzero reserved"},
		{"rootgen zero", func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[4:], 0)
			return v
		}, "rootgen 0"},
		{"count past added", func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[24:], 8)
			return v
		}, "exceeds entries added"},
		{"fence count over cap", func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[76:], uint32(streamFenceMaxRuns+1))
			return v
		}, "out of range"},
		{"torn fence entry", func(v []byte) []byte { return v[:len(v)-1] }, "does not fit"},
		{"trailing bytes", func(v []byte) []byte { return append(v, 0) }, "does not fit"},
		{"zero base", func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(0):], 0)
			binary.LittleEndian.PutUint64(v[fe(0)+8:], 0)
			return v
		}, "zero base ID"},
		{"base order", func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(1):], 9)
			return v
		}, "out of ID order"},
		{"segid past next", func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[fe(2)+16:], 6)
			return v
		}, "at or past next_segid"},
		{"entry count zero", func(v []byte) []byte {
			// Keep the sum right by moving entry 1's count onto entry 0.
			binary.LittleEndian.PutUint32(v[fe(0)+24:], 5)
			binary.LittleEndian.PutUint32(v[fe(1)+24:], 0)
			return v
		}, "count 0"},
		{"count sum drift", func(v []byte) []byte {
			binary.LittleEndian.PutUint32(v[fe(1)+24:], 3)
			return v
		}, "sum to"},
		{"last below tail base", func(v []byte) []byte {
			binary.LittleEndian.PutUint64(v[32:], 39)
			return v
		}, "below the tail run"},
	}
	for _, tc := range cases {
		r := testStreamRoot()
		v := tc.mut(appendStreamRoot(nil, &r))
		if _, err := decodeStreamRoot(v, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}
