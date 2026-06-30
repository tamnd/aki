package lab

import (
	"encoding/binary"
	"hash/maphash"
	"math/bits"
	"testing"
)

// Technique question: which hash for a fixed 16-byte key on the index hot path?
//
// The index hashes the key on every Get and Set, so the hash is on the critical path of
// the whole store. The key here is the 16-byte shape the string benchmark uses (an
// 8-byte id plus an 8-byte mix). Four candidates:
//
//   - fnv1a: the textbook byte-at-a-time hash, a baseline for "obviously too slow".
//   - wordFold: the wyhash-style word-at-a-time mix f1raw and the hot engine use, two
//     64-bit reads folded with a 128-bit multiply. The point of this benchmark is to
//     justify that choice with a number.
//   - mulShift: a minimal two-word multiply-shift, the cheapest thing that still mixes
//     both words, to see how much the extra folding in wordFold actually costs.
//   - maphash: the Go runtime's hash/maphash over the bytes, to check whether the
//     standard library's well-optimized hash beats a hand-rolled one.
//
// Each benchmark hashes a precomputed slice of distinct 16-byte keys and accumulates the
// result so the compiler cannot elide the work.

const hashKeys = 4096

func makeHashKeys() [][]byte {
	keys := make([][]byte, hashKeys)
	for i := range keys {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:8], uint64(i))
		binary.LittleEndian.PutUint64(b[8:16], uint64(i)*0x9e3779b97f4a7c15)
		keys[i] = b
	}
	return keys
}

func fnv1a(b []byte) uint64 {
	const (
		off = 1469598103934665603
		prm = 1099511628211
	)
	h := uint64(off)
	for _, c := range b {
		h ^= uint64(c)
		h *= prm
	}
	return h
}

func mulFold(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func wordFold(b []byte) uint64 {
	const (
		s0 = 0xa0761d6478bd642f
		s1 = 0xe7037ed1a0b428db
		s2 = 0x8ebc6af09c88c6e3
	)
	h := s0 ^ uint64(len(b))
	for len(b) >= 8 {
		h = mulFold(h^binary.LittleEndian.Uint64(b), s1)
		b = b[8:]
	}
	return mulFold(h, s2)
}

func mulShift(b []byte) uint64 {
	w0 := binary.LittleEndian.Uint64(b[0:8])
	w1 := binary.LittleEndian.Uint64(b[8:16])
	return (w0*0x9e3779b97f4a7c15 ^ w1*0xc2b2ae3d27d4eb4f) >> 16
}

func BenchmarkHashFNV1a(b *testing.B) {
	keys := makeHashKeys()
	var acc uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc ^= fnv1a(keys[i&(hashKeys-1)])
	}
	sink = acc
}

func BenchmarkHashWordFold(b *testing.B) {
	keys := makeHashKeys()
	var acc uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc ^= wordFold(keys[i&(hashKeys-1)])
	}
	sink = acc
}

func BenchmarkHashMulShift(b *testing.B) {
	keys := makeHashKeys()
	var acc uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc ^= mulShift(keys[i&(hashKeys-1)])
	}
	sink = acc
}

func BenchmarkHashMaphash(b *testing.B) {
	keys := makeHashKeys()
	seed := maphash.MakeSeed()
	var acc uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc ^= maphash.Bytes(seed, keys[i&(hashKeys-1)])
	}
	sink = acc
}

// sink defeats dead-code elimination of accumulated benchmark results.
var sink uint64
