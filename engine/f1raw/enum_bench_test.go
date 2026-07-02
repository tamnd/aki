package f1raw

import (
	"encoding/binary"
	"strconv"
	"testing"
)

// buildHash builds a single collection of n element rows under one prefix, the
// element-per-row shape HGETALL enumerates, and returns the store and the bounding
// prefix. Keys are uvarint(len(hkey))|hkey|field, matching f1srv's fieldKey layout.
func buildHash(b *testing.B, n int) (*Store, []byte) {
	rec := int(recSize(16, 16))
	s := New(n/4+16, n*rec+n*rec/4+1<<20)
	hkey := []byte("BH")
	var tmp [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(tmp[:], uint64(len(hkey)))
	prefix := append([]byte{}, tmp[:nn]...)
	prefix = append(prefix, hkey...)

	const kindHashField byte = 0x01
	for i := 0; i < n; i++ {
		key := append([]byte{}, prefix...)
		key = append(key, []byte("f"+strconv.Itoa(i))...)
		val := []byte("v" + strconv.Itoa(i))
		if _, err := s.PutKind(key, val, kindHashField); err != nil {
			b.Fatal(err)
		}
		s.CollInsert(key, kindHashField)
	}
	return s, prefix
}

// BenchmarkEnumHGetAll mimics streamHash's HGETALL loop: batched CollScanKV over the
// prefix, reading each field's value from its offset. It isolates the engine
// enumeration cost from the RESP write path.
func BenchmarkEnumHGetAll(b *testing.B) {
	const n = 100000
	s, prefix := buildHash(b, n)
	scanK := make([][]byte, 0, 256)
	scanO := make([]uint64, 0, 256)
	vbuf := make([]byte, 0, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var after []byte
		got := 0
		for {
			keys, offs, last := s.CollScanKV(prefix, after, 256, scanK[:0], scanO[:0])
			if len(keys) == 0 {
				break
			}
			for j := range keys {
				vbuf = s.ReadValueAt(offs[j], vbuf)
				got++
			}
			if last == nil {
				break
			}
			after = last
		}
		if got != n {
			b.Fatalf("enumerated %d, want %d", got, n)
		}
	}
}
