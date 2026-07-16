package store

import (
	"fmt"
	"testing"
)

// The hash must be self-consistent within a run and must actually mix: the
// probe, the router, and the split redistribution all recompute it from the
// key bytes and expect the same word every time.
func TestHashDeterministic(t *testing.T) {
	for _, k := range []string{"", "a", "hot", "user:1000", "a-key-longer-than-eight-bytes"} {
		h1 := Hash([]byte(k))
		h2 := Hash([]byte(k))
		if h1 != h2 {
			t.Fatalf("Hash(%q) = %x then %x", k, h1, h2)
		}
	}
}

// Distinct keys, including prefixes and length-extended variants, must hash to
// distinct words at any realistic count. A collision here is not impossible in
// principle, but 4000 64-bit values colliding would mean the mix is broken.
func TestHashSpreads(t *testing.T) {
	seen := make(map[uint64]string)
	add := func(k string) {
		h := Hash([]byte(k))
		if prev, dup := seen[h]; dup {
			t.Fatalf("Hash collision: %q and %q both %x", prev, k, h)
		}
		seen[h] = k
	}
	for i := 0; i < 1000; i++ {
		add(fmt.Sprintf("key-%d", i))
		add(fmt.Sprintf("key-%d-suffix", i))
		add(fmt.Sprintf("%d-key", i))
		add(fmt.Sprintf("k%07d", i))
	}
}

// The tag is 12 bits, never zero, and derived only from bits the address field
// leaves free, so packing it into an entry word can never clobber the address.
func TestTagShapeAndPacking(t *testing.T) {
	for i := 0; i < 1000; i++ {
		h := Hash([]byte(fmt.Sprintf("key-%d", i)))
		tag := tagOf(h)
		if tag == 0 {
			t.Fatalf("tagOf(%x) = 0, the empty-slot sentinel", h)
		}
		if tag >= 1<<12 {
			t.Fatalf("tagOf(%x) = %x, wider than 12 bits", h, tag)
		}
		addr := uint64(i*8 + 8)
		w := tag<<tagShift | addr
		if w&addrMask != addr {
			t.Fatalf("entry word %x lost address %x", w, addr)
		}
		if w>>tagShift != tag {
			t.Fatalf("entry word %x lost tag %x", w, tag)
		}
	}
}

func BenchmarkHash16(b *testing.B) {
	key := []byte("0123456789abcdef")
	b.ReportAllocs()
	var sink uint64
	for i := 0; i < b.N; i++ {
		sink ^= Hash(key)
	}
	_ = sink
}
