package store

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// refBitOp is the byte-level oracle: the result length is the longest source,
// shorter sources zero-pad, and NOT is the single-source complement at that
// source's length. A zero result length (all sources empty) is the empty slice,
// which the store turns into a destination delete.
func refBitOp(op int, srcs [][]byte) []byte {
	if op == BitNot {
		out := make([]byte, len(srcs[0]))
		for i := range out {
			out[i] = ^srcs[0][i]
		}
		return out
	}
	maxlen := 0
	for _, s := range srcs {
		if len(s) > maxlen {
			maxlen = len(s)
		}
	}
	if maxlen == 0 {
		return nil
	}
	out := make([]byte, maxlen)
	for i := 0; i < maxlen; i++ {
		var acc byte
		if op == BitAnd {
			acc = 0xFF
		}
		for _, s := range srcs {
			var v byte
			if i < len(s) {
				v = s[i]
			}
			switch op {
			case BitAnd:
				acc &= v
			case BitOr:
				acc |= v
			case BitXor:
				acc ^= v
			}
		}
		out[i] = acc
	}
	return out
}

func fillRand(rng *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
}

// TestBitOpModel drives random AND/OR/XOR/NOT over sources spanning the embedded,
// separated, and chunked bands (including missing sources and aliased
// destinations) and checks the stored result and its length against the byte
// oracle. Keys are dropped each round so the arena stays bounded.
func TestBitOpModel(t *testing.T) {
	s := New(64<<20, 1<<20)
	rng := rand.New(rand.NewPCG(0x817e, 0x0b17))
	lens := []int{0, 1, 7, 100, 2000, 80000, 130001}
	ops := []int{BitAnd, BitOr, BitXor, BitNot}
	for iter := 0; iter < 200; iter++ {
		op := ops[iter%len(ops)]
		nsrc := 1 + rng.IntN(3)
		if op == BitNot {
			nsrc = 1
		}
		srcs := make([][]byte, nsrc)
		keys := make([][]byte, nsrc)
		for i := range srcs {
			L := lens[rng.IntN(len(lens))]
			b := make([]byte, L)
			fillRand(rng, b)
			srcs[i] = b
			keys[i] = []byte(fmt.Sprintf("s%d_%d", iter, i))
			// A zero-length source stays a missing key (an absent source reads
			// empty), which the oracle already models as the empty slice.
			if L > 0 {
				if err := s.Set(keys[i], b); err != nil {
					t.Fatalf("iter %d set: %v", iter, err)
				}
			}
		}
		dest := []byte(fmt.Sprintf("d%d", iter))
		if rng.IntN(3) == 0 {
			// Exercise the aliased-destination path: dest is one of the sources.
			dest = keys[rng.IntN(nsrc)]
		}
		want := refBitOp(op, srcs)
		n, err := s.BitOp(op, dest, keys, 0)
		if err != nil {
			t.Fatalf("iter %d BitOp: %v", iter, err)
		}
		if int(n) != len(want) {
			t.Fatalf("iter %d op %d len: got %d want %d", iter, op, n, len(want))
		}
		got, ok := s.Get(dest, nil)
		if len(want) == 0 {
			if ok && len(got) > 0 {
				t.Fatalf("iter %d empty result should delete dest, got %d bytes", iter, len(got))
			}
		} else if !bytes.Equal(got, want) {
			t.Fatalf("iter %d op %d mismatch: got %d bytes want %d", iter, op, len(got), len(want))
		}
		seen := map[string]bool{}
		for _, k := range keys {
			if !seen[string(k)] {
				s.Del(k, 0)
				seen[string(k)] = true
			}
		}
		if !seen[string(dest)] {
			s.Del(dest, 0)
		}
	}
}

// TestBitOpSparseHoles runs the ops over a sparse source built by two far-apart
// SETBITs, so the value carries directory holes between the live chunks. The
// oracle reads each source's materialized logical bytes, so holes read as zero
// exactly as the byte model expects, and the AND short-circuit, the OR carry,
// and the NOT densification are all checked against it.
func TestBitOpSparseHoles(t *testing.T) {
	s := New(64<<20, 1<<20)
	// a: bit 5 in byte 0 and bit 3 in byte 130000, holes between.
	if _, err := s.SetBit([]byte("a"), 5, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetBit([]byte("a"), 130000*8+3, 1, 0); err != nil {
		t.Fatal(err)
	}
	// b: a short dense mask.
	if err := s.Set([]byte("b"), bytes.Repeat([]byte{0xFF}, 10)); err != nil {
		t.Fatal(err)
	}
	aLog, _ := s.Get([]byte("a"), nil)
	bLog, _ := s.Get([]byte("b"), nil)
	aCopy := append([]byte(nil), aLog...)
	bCopy := append([]byte(nil), bLog...)

	cases := []struct {
		op   int
		srcs [][]byte
	}{
		{BitAnd, [][]byte{aCopy, bCopy}},
		{BitOr, [][]byte{aCopy, bCopy}},
		{BitXor, [][]byte{aCopy, bCopy}},
		{BitNot, [][]byte{aCopy}},
	}
	for _, c := range cases {
		keys := [][]byte{[]byte("a"), []byte("b")}[:len(c.srcs)]
		want := refBitOp(c.op, c.srcs)
		n, err := s.BitOp(c.op, []byte("dst"), keys, 0)
		if err != nil {
			t.Fatalf("op %d: %v", c.op, err)
		}
		if int(n) != len(want) {
			t.Fatalf("op %d len: got %d want %d", c.op, n, len(want))
		}
		got, _ := s.Get([]byte("dst"), nil)
		if !bytes.Equal(got, want) {
			t.Fatalf("op %d mismatch at first differing byte", c.op)
		}
	}
}

// TestBitOpAllMissingDeletes pins that BITOP over only missing sources deletes
// the destination and reports 0, even when the destination previously held a
// value.
func TestBitOpAllMissingDeletes(t *testing.T) {
	s := New(1<<20, 1<<16)
	if err := s.Set([]byte("dst"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	n, err := s.BitOp(BitOr, []byte("dst"), [][]byte{[]byte("gone1"), []byte("gone2")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("all-missing length: got %d want 0", n)
	}
	if _, ok := s.Get([]byte("dst"), nil); ok {
		t.Fatalf("destination should be deleted")
	}
}
