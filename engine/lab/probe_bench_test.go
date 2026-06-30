package lab

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

// Technique question: is the 16-bit tag in each index entry worth it?
//
// f1raw packs a 16-bit tag (high hash bits) and a 48-bit record address into each
// 64-bit index entry. On a probe, a slot whose tag does not match the key's tag is
// rejected without touching the arena at all; only a tag match dereferences the record
// to compare the full key. The alternative is to store the address alone and compare
// the key on every non-empty slot.
//
// The tag matters most exactly when a bucket is full of collisions and the wanted key
// is late in the scan, because then the no-tag variant pays a key compare (and an arena
// cache miss) on every slot it skips, while the tag variant pays one cheap register
// compare per skip. This benchmark builds a full 7-entry bucket whose keys all land in
// it, puts the target in the last slot, and probes for it, so every probe scans six
// non-matching slots before the hit. The records live in a separate arena so a key
// compare is a real pointer chase, the way it is in the store.

const (
	slots   = 7
	recLen  = 24 // 16-byte key + 8 bytes padding, like a small record header
	tagMask = 0xffff
)

type probeBucket struct {
	entry [slots]uint64 // tag<<48 | addr  (or just addr in the no-tag variant)
}

func buildProbe() (*probeBucket, []byte, []byte) {
	arena := make([]byte, slots*recLen)
	var bWith, bNo probeBucket
	var target []byte
	for i := 0; i < slots; i++ {
		key := make([]byte, 16)
		binary.LittleEndian.PutUint64(key[0:8], uint64(i+1))
		binary.LittleEndian.PutUint64(key[8:16], uint64(i+1)*0x9e3779b97f4a7c15)
		off := uint64(i * recLen)
		copy(arena[off:], key)
		h := wordFold(key)
		bWith.entry[i] = (h&tagMask)<<48 | off
		bNo.entry[i] = off
		if i == slots-1 {
			target = key
		}
	}
	// Return both buckets via one struct each; the caller picks.
	withCopy := bWith
	noCopy := bNo
	_ = noCopy
	return &withCopy, arena, target
}

func keyAt(arena []byte, off uint64) []byte { return arena[off : off+16] }

// findWithTag rejects on the tag before any arena access, dereferencing only on a tag
// match.
func findWithTag(bk *probeBucket, arena, key []byte, h uint64) (uint64, bool) {
	tag := h & tagMask
	for i := 0; i < slots; i++ {
		e := bk.entry[i]
		if e>>48 != tag {
			continue
		}
		off := e & ((1 << 48) - 1)
		if eq16(keyAt(arena, off), key) {
			return off, true
		}
	}
	return 0, false
}

// findNoTag compares the full key on every non-empty slot.
func findNoTag(bk *probeBucket, arena, key []byte) (uint64, bool) {
	for i := 0; i < slots; i++ {
		off := bk.entry[i]
		if eq16(keyAt(arena, off), key) {
			return off, true
		}
	}
	return 0, false
}

func eq16(a, b []byte) bool {
	return *(*uint64)(unsafe.Pointer(&a[0])) == *(*uint64)(unsafe.Pointer(&b[0])) &&
		*(*uint64)(unsafe.Pointer(&a[8])) == *(*uint64)(unsafe.Pointer(&b[8]))
}

func BenchmarkProbeWithTag(b *testing.B) {
	bk, arena, target := buildProbe()
	h := wordFold(target)
	var hits int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := findWithTag(bk, arena, target, h); ok {
			hits++
		}
	}
	if hits != b.N {
		b.Fatalf("missed: %d of %d", b.N-hits, b.N)
	}
}

func BenchmarkProbeNoTag(b *testing.B) {
	bkWith, arena, target := buildProbe()
	// Rebuild a no-tag bucket (addr only) from the same arena layout.
	var bk probeBucket
	for i := 0; i < slots; i++ {
		bk.entry[i] = bkWith.entry[i] & ((1 << 48) - 1)
	}
	var hits int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := findNoTag(&bk, arena, target); ok {
			hits++
		}
	}
	if hits != b.N {
		b.Fatalf("missed: %d of %d", b.N-hits, b.N)
	}
}
