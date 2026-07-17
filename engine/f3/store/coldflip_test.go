package store

import (
	"bytes"
	"fmt"
	"testing"
)

// The cold demote flip: a store opened over an .aki cold region moves an
// eligible resident string into the shared cold-chunk region through the akicold
// adapter, and a later read serves the frame back through the same seam. These
// tests drive the synchronous demote and cold-read path (DemoteKey, DemoteCold,
// GetString's cold arm) end to end over a real .aki, reusing the flip store from
// bandsflip_test.go.
//
// The demote is eager here, one cold_chunk segment per record. The batched form,
// one segment per migration quantum, rides in with the async drain in a later
// slice.

// TestDemoteFlipsEmbeddedToAkiCold demotes a small embedded string and reads it
// back: the frame lands in the .aki cold region, the census and region size move,
// and the cold read serves the value from the shared region.
func TestDemoteFlipsEmbeddedToAkiCold(t *testing.T) {
	s := newFlipStore(t)

	val := []byte("hello cold world")
	if err := s.Set([]byte("k"), val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if !s.DemoteKey([]byte("k")) {
		t.Fatal("DemoteKey did not move the key")
	}
	cs := s.Cold()
	if cs.Records != 1 {
		t.Fatalf("cold records = %d, want 1", cs.Records)
	}
	if cs.RegionSize == 0 {
		t.Fatal("cold region size = 0 after a demote")
	}

	got, ok := s.Get([]byte("k"), nil)
	if !ok {
		t.Fatal("get missed the cold key")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("cold read back %q, want %q", got, val)
	}
}

// TestDemoteFlipsIntToAkiCold demotes an int-band value, the other self-contained
// band the cold plane admits, and reads its canonical text back from the region.
func TestDemoteFlipsIntToAkiCold(t *testing.T) {
	s := newFlipStore(t)

	if err := s.Set([]byte("n"), []byte("12345")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if !s.DemoteKey([]byte("n")) {
		t.Fatal("DemoteKey did not move the int key")
	}
	got, ok := s.Get([]byte("n"), nil)
	if !ok || !bytes.Equal(got, []byte("12345")) {
		t.Fatalf("cold int read = %q/%v, want 12345", got, ok)
	}
}

// TestDemoteColdSweepAki demotes a batch of eligible keys through the store-level
// sweep and confirms each reads back from the .aki cold region.
func TestDemoteColdSweepAki(t *testing.T) {
	s := newFlipStore(t)

	const n = 20
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		val := []byte(fmt.Sprintf("val-%02d-embedded", i))
		if err := s.Set(key, val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	moved := s.DemoteCold()
	if moved != n {
		t.Fatalf("DemoteCold moved %d, want %d", moved, n)
	}
	if got := s.Cold().Records; got != n {
		t.Fatalf("cold records = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		want := []byte(fmt.Sprintf("val-%02d-embedded", i))
		got, ok := s.Get(key, nil)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("cold read %d = %q/%v, want %q", i, got, ok, want)
		}
	}
}
