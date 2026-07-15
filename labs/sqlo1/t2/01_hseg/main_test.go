package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs one small cell end to end on both arms and
// checks the CSV shape: nineteen columns, the store value first, the
// load, hset, hget, and flush rows all present, and the numeric fields
// parse.
func TestRunAllSmoke(t *testing.T) {
	for _, arm := range []string{"a", "b"} {
		t.Run(arm, func(t *testing.T) {
			cfg := config{
				dir: t.TempDir(), store: arm, segMax: 1024, fdist: "small", setpct: 50,
				dist: "zipf", keys: 2, fields: 800, ops: 4000,
				threshold: 1 << 20, ckpt: 8,
			}
			var out bytes.Buffer
			if err := runAll(cfg, &out); err != nil {
				t.Fatalf("runAll: %v", err)
			}
			want := map[string]bool{"load": false, "hset": false, "hget": false, "flush": false}
			for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
				fields := strings.Split(line, ",")
				if len(fields) != 19 {
					t.Fatalf("row has %d fields, want 19: %q", len(fields), line)
				}
				if fields[0] != arm {
					t.Fatalf("row carries store %q, want %q: %q", fields[0], arm, line)
				}
				if _, ok := want[fields[7]]; ok {
					want[fields[7]] = true
				}
				for _, idx := range []int{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18} {
					if _, err := strconv.ParseFloat(fields[idx], 64); err != nil {
						t.Fatalf("field %d not numeric in %q: %v", idx, line, err)
					}
				}
			}
			for w, seen := range want {
				if !seen {
					t.Fatalf("workload %s missing from output:\n%s", w, out.String())
				}
			}
		})
	}
}

type decodedRoot struct {
	count     int64
	nextSegid uint64
	fence     []fenceEnt
	fill      []int
}

func decodeRoot(t *testing.T, buf []byte) decodedRoot {
	t.Helper()
	if len(buf) < rootHdrSize {
		t.Fatalf("root payload too short: %d bytes", len(buf))
	}
	if buf[0] != 2 {
		t.Fatalf("root sub = %d, want 2 (segmented)", buf[0])
	}
	r := decodedRoot{
		count:     int64(binary.LittleEndian.Uint64(buf[8:])),
		nextSegid: binary.LittleEndian.Uint64(buf[16:]),
	}
	segCount := int(binary.LittleEndian.Uint32(buf[32:]))
	if want := rootHdrSize + fenceEntSize*segCount; len(buf) != want {
		t.Fatalf("root payload %d bytes, want %d for %d fence entries", len(buf), want, segCount)
	}
	for i := range segCount {
		off := rootHdrSize + fenceEntSize*i
		packed := binary.LittleEndian.Uint64(buf[off+8:])
		r.fence = append(r.fence, fenceEnt{
			lo:    binary.LittleEndian.Uint64(buf[off:]),
			segid: packed & (1<<48 - 1),
		})
		r.fill = append(r.fill, int(packed>>48))
	}
	return r
}

func decodeSeg(t *testing.T, buf []byte) []entry {
	t.Helper()
	if len(buf) < segHdrSize {
		t.Fatalf("segment payload too short: %d bytes", len(buf))
	}
	n := int(binary.LittleEndian.Uint16(buf))
	var entries []entry
	off := segHdrSize
	for range n {
		flen := int(binary.LittleEndian.Uint16(buf[off+1:]))
		vlen := int(binary.LittleEndian.Uint32(buf[off+3:]))
		field := buf[off+7 : off+7+flen]
		entries = append(entries, entry{
			fh:    fh(field),
			field: field,
			value: buf[off+7+flen : off+7+flen+vlen],
		})
		off += 7 + flen + vlen
	}
	if off != len(buf) {
		t.Fatalf("segment payload %d bytes, decoded %d", len(buf), off)
	}
	return entries
}

// TestSegmentOracle drives the model on both arms with random HSETs
// and flushes at a tiny seg_max so splits are constant, mirrors every
// write on a reference map, then reads the drained state back through
// the store probes only: the fence must partition the space, every
// segment must sit under seg_max with sorted in-range entries and an
// honest fill class, and the union of entries must equal the reference
// exactly, with the root count to match.
func TestSegmentOracle(t *testing.T) {
	for _, arm := range []string{"a", "b"} {
		t.Run(arm, func(t *testing.T) { segmentOracle(t, arm) })
	}
}

func segmentOracle(t *testing.T, arm string) {
	const segMax = 512
	const nFields = 600
	cfg := config{store: arm, segMax: segMax, ckpt: 8}
	path := filepath.Join(t.TempDir(), "oracle.db")
	keys := [][]byte{[]byte("h:oracle")}
	st, err := openStore(cfg, path, keys)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	defer st.close()

	m := &model{st: st, cfg: cfg, hs: []*hash{newHash(keys[0])}}
	rng := rand.New(rand.NewSource(31))
	names := make([][]byte, nFields)
	for i := range names {
		names[i] = []byte("f" + strconv.Itoa(i) + strings.Repeat("x", rng.Intn(12)))
	}
	ref := map[string][]byte{}
	for i := range 8000 {
		name := names[rng.Intn(nFields)]
		v := make([]byte, 4+rng.Intn(61))
		for j := range v {
			v[j] = byte('a' + rng.Intn(26))
		}
		m.hset(0, name, v)
		ref[string(name)] = v
		if got := m.hget(0, name); !bytes.Equal(got, v) {
			t.Fatalf("hget after hset: got %q, want %q", got, v)
		}
		if i%700 == 699 {
			if err := m.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}
	if err := m.flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if m.splits == 0 {
		t.Fatal("no splits at segMax 512; the oracle exercised nothing")
	}

	rootBuf, err := st.rootGet(0)
	if err != nil {
		t.Fatalf("rootGet: %v", err)
	}
	if rootBuf == nil {
		t.Fatal("root record missing")
	}
	root := decodeRoot(t, rootBuf)

	if root.count != int64(len(ref)) {
		t.Fatalf("root count = %d, reference has %d fields", root.count, len(ref))
	}
	if root.fence[0].lo != 0 {
		t.Fatalf("first fence lo = %d, want 0", root.fence[0].lo)
	}
	got := map[string][]byte{}
	for i, fe := range root.fence {
		if i > 0 && fe.lo <= root.fence[i-1].lo {
			t.Fatalf("fence not strictly sorted at %d: %d after %d", i, fe.lo, root.fence[i-1].lo)
		}
		if fe.segid >= root.nextSegid {
			t.Fatalf("fence segid %d >= next_segid %d", fe.segid, root.nextSegid)
		}
		hi := uint64(1<<64 - 1)
		if i+1 < len(root.fence) {
			hi = root.fence[i+1].lo
		}
		row, err := st.segGet(0, fe.segid)
		if err != nil {
			t.Fatalf("segGet: %v", err)
		}
		if row == nil {
			t.Fatalf("segment record %d missing", fe.segid)
		}
		if len(row) > segMax {
			t.Fatalf("segment %d is %d bytes on disk, over seg_max %d", fe.segid, len(row), segMax)
		}
		entries := decodeSeg(t, row)
		if root.fill[i] != len(entries) {
			t.Fatalf("segment %d fill class %d, holds %d entries", fe.segid, root.fill[i], len(entries))
		}
		for j, e := range entries {
			if j > 0 && e.fh < entries[j-1].fh {
				t.Fatalf("segment %d entries not sorted by fh at %d", fe.segid, j)
			}
			if e.fh < fe.lo || (i+1 < len(root.fence) && e.fh >= hi) {
				t.Fatalf("segment %d entry fh %d outside [%d, %d)", fe.segid, e.fh, fe.lo, hi)
			}
			if _, dup := got[string(e.field)]; dup {
				t.Fatalf("field %q appears in two segments", e.field)
			}
			got[string(e.field)] = slices.Clone(e.value)
		}
	}
	if len(got) != len(ref) {
		t.Fatalf("readback has %d fields, reference %d", len(got), len(ref))
	}
	for name, v := range ref {
		if !bytes.Equal(got[name], v) {
			t.Fatalf("field %q: readback %q, reference %q", name, got[name], v)
		}
	}
}
