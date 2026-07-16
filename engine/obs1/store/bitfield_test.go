package store

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// refFieldGet reads a width-bit field MSB-first from the logical bytes, zeros
// past the end, the reference FieldGet must match.
func refFieldGet(raw []byte, off int64, width uint) uint64 {
	var v uint64
	for i := uint(0); i < width; i++ {
		gb := off + int64(i)
		var bit uint64
		if bi := gb >> 3; bi < int64(len(raw)) {
			bit = uint64(raw[bi]>>(7-uint(gb&7))) & 1
		}
		v = (v << 1) | bit
	}
	return v
}

// refFieldSet writes the low width bits of val into the model, growing it to
// cover the field, the reference FieldSet must match byte for byte.
func refFieldSet(raw *[]byte, off int64, width uint, val uint64) {
	if last := (off + int64(width) - 1) >> 3; last >= int64(len(*raw)) {
		grow := make([]byte, last+1)
		copy(grow, *raw)
		*raw = grow
	}
	for i := uint(0); i < width; i++ {
		gb := off + int64(i)
		mask := byte(1) << (7 - uint(gb&7))
		if (val>>(width-1-i))&1 == 1 {
			(*raw)[gb>>3] |= mask
		} else {
			(*raw)[gb>>3] &^= mask
		}
	}
}

func TestFieldGetSetModel(t *testing.T) {
	s := New(64<<20, 1<<20)
	key := []byte("bf")
	var model []byte
	rng := rand.New(rand.NewPCG(0x51ed, 0x2764))
	for iter := 0; iter < 3000; iter++ {
		width := uint(1 + rng.IntN(64))
		// Offsets up to ~3M bits so the value spills into chunks and fields
		// straddle chunk boundaries.
		off := rng.Int64N(3_000_000)
		mask := ^uint64(0)
		if width < 64 {
			mask = (uint64(1) << width) - 1
		}
		val := rng.Uint64() & mask
		if err := s.FieldSet(key, off, width, val, 0); err != nil {
			t.Fatalf("iter %d FieldSet: %v", iter, err)
		}
		refFieldSet(&model, off, width, val)
		if got := s.FieldGet(key, off, width, 0); got != (val & mask) {
			t.Fatalf("iter %d readback off=%d w=%d: got %d want %d", iter, off, width, got, val&mask)
		}
		// Probe an unrelated field against the model.
		pw := uint(1 + rng.IntN(64))
		po := rng.Int64N(3_000_000)
		if got, want := s.FieldGet(key, po, pw, 0), refFieldGet(model, po, pw); got != want {
			t.Fatalf("iter %d probe off=%d w=%d: got %d want %d", iter, po, pw, got, want)
		}
	}
	got, _ := s.Get(key, nil)
	if !bytes.Equal(got, model) {
		t.Fatalf("final value mismatch: len got %d want %d", len(got), len(model))
	}
}

// TestFieldStraddlesChunkBoundary pins a field that spans two chunks: it writes
// across the 64 KiB chunk edge and reads the value back whole.
func TestFieldStraddlesChunkBoundary(t *testing.T) {
	s := New(64<<20, 1<<20)
	key := []byte("edge")
	// Byte 65535 is the last of chunk 0, byte 65536 the first of chunk 1; a 24-bit
	// field starting four bits before that boundary crosses it.
	off := int64(65536*8 - 4)
	if err := s.FieldSet(key, off, 24, 0xABCDEF&0xFFFFFF, 0); err != nil {
		t.Fatalf("FieldSet: %v", err)
	}
	if got := s.FieldGet(key, off, 24, 0); got != 0xABCDEF {
		t.Fatalf("straddle readback: got %06x want ABCDEF", got)
	}
	// A read of the same range from a fresh field on either side stays zero.
	if got := s.FieldGet(key, 0, 8, 0); got != 0 {
		t.Fatalf("head byte should be zero, got %d", got)
	}
}

// TestFieldGetMissingKeyZero pins that a field read on an absent key is zero and
// never creates it.
func TestFieldGetMissingKeyZero(t *testing.T) {
	s := New(1<<20, 1<<16)
	if got := s.FieldGet([]byte("nope"), 40, 16, 0); got != 0 {
		t.Fatalf("missing-key field: got %d want 0", got)
	}
	if s.Len() != 0 {
		t.Fatalf("field read created the key")
	}
}
