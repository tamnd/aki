package store

import (
	"math/bits"
	"math/rand/v2"
	"testing"
)

// naiveBitCount is the reference: count set bits in raw[lo..hi] with byte lo
// masked by firstMask and byte hi by lastMask, the same boundary contract the
// kernel promises.
func naiveBitCount(raw []byte, lo, hi int64, firstMask, lastMask byte) int64 {
	var sum int64
	for i := lo; i <= hi; i++ {
		b := raw[i]
		if i == lo {
			b &= firstMask
		}
		if i == hi {
			b &= lastMask
		}
		sum += int64(bits.OnesCount8(b))
	}
	return sum
}

// naiveBitPos is the reference: the absolute offset of the first bit equal to
// bit in raw[lo..hi], out-of-range boundary bits forced to the opposite value.
func naiveBitPos(raw []byte, bit int, lo, hi int64, firstMask, lastMask byte) int64 {
	for i := lo; i <= hi; i++ {
		b := raw[i]
		if bit == 1 {
			if i == lo {
				b &= firstMask
			}
			if i == hi {
				b &= lastMask
			}
		} else {
			if i == lo {
				b |= ^firstMask
			}
			if i == hi {
				b |= ^lastMask
			}
		}
		if p := firstBitInByte(b, bit); p >= 0 {
			return i*8 + int64(p)
		}
	}
	return -1
}

// bitKernelCase is one stored value plus the raw bytes it should read back as,
// so the kernel can be checked against the naive reference over the true bytes.
type bitKernelCase struct {
	name string
	key  []byte
	raw  []byte
}

// buildBitKernelCases stores values spanning every string band and returns the
// bytes each should render as, holes included.
func buildBitKernelCases(t *testing.T, s *Store) []bitKernelCase {
	t.Helper()
	rng := rand.New(rand.NewPCG(0x9e3779b9, 0x7f4a7c15))
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	var cases []bitKernelCase
	put := func(name, key string, raw []byte) {
		if err := s.Set([]byte(key), raw); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
		cases = append(cases, bitKernelCase{name, []byte(key), raw})
	}
	put("embedded-small", "e0", []byte("hello world foobar"))
	put("embedded-rand", "e1", randBytes(200))
	put("separated", "s0", randBytes(4000))
	put("chunked-dense", "c0", randBytes(200000))

	// A sparse chunked value: set two far-apart bits, leaving all-zero holes
	// between them, then read the logical bytes back for the reference.
	sparse := "sp"
	if _, err := s.SetBit([]byte(sparse), 5, 1, 0); err != nil {
		t.Fatalf("setbit sparse: %v", err)
	}
	if _, err := s.SetBit([]byte(sparse), 1<<20+3, 1, 0); err != nil {
		t.Fatalf("setbit sparse hi: %v", err)
	}
	if _, err := s.SetBit([]byte(sparse), 700003, 1, 0); err != nil {
		t.Fatalf("setbit sparse mid: %v", err)
	}
	raw, ok := s.Get([]byte(sparse), nil)
	if !ok {
		t.Fatalf("get sparse")
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	cases = append(cases, bitKernelCase{"chunked-sparse", []byte(sparse), cp})
	return cases
}

func TestBitCountMatchesNaive(t *testing.T) {
	s := New(64<<20, 1<<20)
	cases := buildBitKernelCases(t, s)
	rng := rand.New(rand.NewPCG(1, 2))
	for _, c := range cases {
		n := int64(len(c.raw))
		if n == 0 {
			continue
		}
		// Whole-value count, the BYTE-range-with-no-args form.
		if got, want := s.BitCount(c.key, 0, n-1, 0xFF, 0xFF, 0), naiveBitCount(c.raw, 0, n-1, 0xFF, 0xFF); got != want {
			t.Fatalf("%s whole: got %d want %d", c.name, got, want)
		}
		for i := 0; i < 400; i++ {
			lo := rng.Int64N(n)
			hi := lo + rng.Int64N(n-lo)
			fm := byte(0xFF)
			lm := byte(0xFF)
			if rng.Uint32()&1 == 0 {
				fm = byte(0xFF) >> (rng.Uint32() % 8)
				lm = byte(0xFF) << (rng.Uint32() % 8)
			}
			got := s.BitCount(c.key, lo, hi, fm, lm, 0)
			want := naiveBitCount(c.raw, lo, hi, fm, lm)
			if got != want {
				t.Fatalf("%s [%d,%d] fm=%08b lm=%08b: got %d want %d", c.name, lo, hi, fm, lm, got, want)
			}
		}
	}
}

func TestBitPosMatchesNaive(t *testing.T) {
	s := New(64<<20, 1<<20)
	cases := buildBitKernelCases(t, s)
	rng := rand.New(rand.NewPCG(3, 4))
	for _, c := range cases {
		n := int64(len(c.raw))
		if n == 0 {
			continue
		}
		for _, bit := range []int{0, 1} {
			for i := 0; i < 400; i++ {
				lo := rng.Int64N(n)
				hi := lo + rng.Int64N(n-lo)
				fm := byte(0xFF)
				lm := byte(0xFF)
				if rng.Uint32()&1 == 0 {
					fm = byte(0xFF) >> (rng.Uint32() % 8)
					lm = byte(0xFF) << (rng.Uint32() % 8)
				}
				got := s.BitPos(c.key, bit, lo, hi, fm, lm, 0)
				want := naiveBitPos(c.raw, bit, lo, hi, fm, lm)
				if got != want {
					t.Fatalf("%s bit=%d [%d,%d] fm=%08b lm=%08b: got %d want %d", c.name, bit, lo, hi, fm, lm, got, want)
				}
			}
		}
	}
}

func TestBitKernelMissingKey(t *testing.T) {
	s := New(1<<20, 1<<16)
	if got := s.BitCount([]byte("nope"), 0, 100, 0xFF, 0xFF, 0); got != 0 {
		t.Fatalf("missing BitCount: got %d want 0", got)
	}
	if got := s.BitPos([]byte("nope"), 1, 0, 100, 0xFF, 0xFF, 0); got != -1 {
		t.Fatalf("missing BitPos: got %d want -1", got)
	}
}
