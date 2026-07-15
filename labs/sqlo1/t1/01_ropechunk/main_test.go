package main

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs every mix at tiny counts and checks the CSV shape:
// sixteen columns, the load, write, read, and flush rows all present, and
// the numeric fields parse.
func TestRunAllSmoke(t *testing.T) {
	for _, mix := range []string{"setrange", "append", "setbit", "getrange"} {
		t.Run(mix, func(t *testing.T) {
			cfg := config{
				dir: t.TempDir(), chunk: 8 << 10, mix: mix, dist: "zipf",
				keys: 2, valMB: 1, ops: 400, wlen: 64, rlen: 512,
				threshold: 256 << 10, ckpt: 4,
			}
			var out bytes.Buffer
			if err := runAll(cfg, &out); err != nil {
				t.Fatalf("runAll: %v", err)
			}
			want := map[string]bool{
				"load": false, mix + "-write": false, mix + "-read": false, "flush": false,
			}
			for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
				fields := strings.Split(line, ",")
				if len(fields) != 16 {
					t.Fatalf("row has %d fields, want 16: %q", len(fields), line)
				}
				if _, ok := want[fields[5]]; ok {
					want[fields[5]] = true
				}
				for _, idx := range []int{7, 8, 9, 10, 11, 12} {
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

// TestRopeOracle drives the rope model with random SETRANGE, APPEND,
// SETBIT, and GETRANGE ops against a flat byte-slice reference, checking
// reads mid-stream, then flushes, drops the overlay, and reads everything
// back through SQL alone. Lazy zero-fill, chunk trimming, RMW bases, and
// the pc column all have to agree with the reference or the sweep would
// be timing a rope that computes the wrong bytes.
func TestRopeOracle(t *testing.T) {
	const chunk = 4 << 10
	cfg := config{chunk: chunk, mix: "setbit", keys: 3, threshold: 1 << 30, ckpt: 1 << 30}
	path := filepath.Join(t.TempDir(), "oracle.db")
	d, err := openDB(path)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer d.close()
	keys := [][]byte{[]byte("r:0000"), []byte("r:0001"), []byte("r:0002")}
	r := newRope(d, cfg, keys)
	ref := make([][]byte, len(keys))

	refSet := func(ki int, off int64, data []byte) {
		if end := off + int64(len(data)); end > int64(len(ref[ki])) {
			ref[ki] = append(ref[ki], make([]byte, end-int64(len(ref[ki])))...)
		}
		copy(ref[ki][off:], data)
	}

	rng := rand.New(rand.NewSource(5))
	for i := range 3000 {
		ki := rng.Intn(len(keys))
		limit := int64(len(ref[ki])) + 3*chunk/2
		switch rng.Intn(5) {
		case 0:
			data := make([]byte, 1+rng.Intn(2*chunk))
			rng.Read(data)
			off := rng.Int63n(limit + 1)
			if err := r.setRange(ki, off, data); err != nil {
				t.Fatalf("setRange: %v", err)
			}
			refSet(ki, off, data)
		case 1:
			data := make([]byte, 1+rng.Intn(300))
			rng.Read(data)
			if err := r.setRange(ki, r.totalLen[ki], data); err != nil {
				t.Fatalf("append: %v", err)
			}
			refSet(ki, int64(len(ref[ki])), data)
		case 2:
			bit := rng.Int63n((limit + 1) * 8)
			val := rng.Intn(2) == 0
			if err := r.setBit(ki, bit, val); err != nil {
				t.Fatalf("setBit: %v", err)
			}
			if bit/8+1 > int64(len(ref[ki])) {
				refSet(ki, bit/8, []byte{0})
			}
			mask := byte(1) << (7 - bit&7)
			if val {
				ref[ki][bit/8] |= mask
			} else {
				ref[ki][bit/8] &^= mask
			}
		case 3:
			if len(ref[ki]) == 0 {
				continue
			}
			off := rng.Int63n(int64(len(ref[ki])))
			n := 1 + rng.Int63n(2*chunk)
			got, err := r.getRange(ki, off, n)
			if err != nil {
				t.Fatalf("getRange: %v", err)
			}
			want := ref[ki][off:min(off+n, int64(len(ref[ki])))]
			if !bytes.Equal(got, want) {
				t.Fatalf("op %d: getRange(%d, %d, %d) mismatch: got %d bytes, want %d", i, ki, off, n, len(got), len(want))
			}
		case 4:
			bit := rng.Int63n((limit + 1) * 8)
			got, err := r.getBit(ki, bit)
			if err != nil {
				t.Fatalf("getBit: %v", err)
			}
			want := false
			if bit/8 < int64(len(ref[ki])) {
				want = ref[ki][bit/8]&(1<<(7-bit&7)) != 0
			}
			if got != want {
				t.Fatalf("op %d: getBit(%d, %d) = %v, want %v", i, ki, bit, got, want)
			}
		}
		if r.totalLen[ki] != int64(len(ref[ki])) {
			t.Fatalf("op %d: totalLen[%d] = %d, reference %d", i, ki, r.totalLen[ki], len(ref[ki]))
		}
		if rng.Intn(200) == 0 {
			if err := r.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}
	if err := r.flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	// Read back through SQL alone: a fresh rope over the same store with
	// an empty overlay, plus the pc column and stored row lengths checked
	// against the reference chunk by chunk.
	pcStmt, _, err := d.conn.Prepare(`SELECT pc, length(v) FROM chunk WHERE k = ?1 AND cid = ?2`)
	if err != nil {
		t.Fatalf("prepare pc check: %v", err)
	}
	defer pcStmt.Close()
	r2 := newRope(d, cfg, keys)
	for ki := range keys {
		r2.totalLen[ki] = int64(len(ref[ki]))
		got, err := r2.getRange(ki, 0, int64(len(ref[ki])))
		if err != nil {
			t.Fatalf("readback getRange: %v", err)
		}
		if !bytes.Equal(got, ref[ki]) {
			t.Fatalf("key %d: stored value diverges from reference (%d vs %d bytes)", ki, len(got), len(ref[ki]))
		}
		for cid := int64(0); cid*chunk < int64(len(ref[ki])); cid++ {
			if err := pcStmt.BindBlob(1, keys[ki]); err != nil {
				t.Fatal(err)
			}
			if err := pcStmt.BindInt64(2, cid); err != nil {
				t.Fatal(err)
			}
			pc, rowLen := int64(0), int64(0)
			if pcStmt.Step() {
				pc, rowLen = pcStmt.ColumnInt64(0), pcStmt.ColumnInt64(1)
			}
			if err := pcStmt.Err(); err != nil {
				t.Fatalf("pc check step: %v", err)
			}
			if err := pcStmt.Reset(); err != nil {
				t.Fatalf("pc check reset: %v", err)
			}
			end := min((cid+1)*chunk, int64(len(ref[ki])))
			if rowLen == 0 {
				continue
			}
			if want := int64(popcount(ref[ki][cid*chunk : end])); pc != want {
				t.Fatalf("key %d chunk %d: pc = %d, reference popcount %d", ki, cid, pc, want)
			}
			if wantLen := end - cid*chunk; rowLen != wantLen {
				t.Fatalf("key %d chunk %d: row length %d, want %d", ki, cid, rowLen, wantLen)
			}
		}
	}
}
