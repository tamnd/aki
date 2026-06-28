package collset_test

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/tamnd/aki/collset"
	"github.com/tamnd/aki/store"
)

// These benchmarks run the segmented set against the real store and against the
// cell form (the whole set in one store value, the current hybrid path) so the
// element-per-row win is a measured number on the engine, not a model. Run:
//
//	go test ./collset/ -run x -bench . -benchmem
//
// The signal is how SADD and SISMEMBER cost scales with set size: the cell form
// rewrites and re-decodes the whole set per op, the segmented set touches one
// segment.

func newStore(b *testing.B) *store.Store {
	b.Helper()
	st, err := store.New(store.Tunables{Shards: 8, PageSize: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	return st
}

func members(n int) [][]byte {
	m := make([][]byte, n)
	for i := range m {
		m[i] = []byte("member:" + strconv.Itoa(i))
	}
	return m
}

// --- segmented set over the real store ---

func benchAddSegmented(b *testing.B, n int) {
	ms := members(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		st := newStore(b)
		set := collset.New(st, 1)
		for _, m := range ms {
			if _, err := set.Add(m); err != nil {
				b.Fatal(err)
			}
		}
		st.Close()
	}
}

func BenchmarkAddSegmented100(b *testing.B)  { benchAddSegmented(b, 100) }
func BenchmarkAddSegmented1000(b *testing.B) { benchAddSegmented(b, 1000) }

func benchIsMemberSegmented(b *testing.B, n int) {
	st := newStore(b)
	defer st.Close()
	set := collset.New(st, 1)
	ms := members(n)
	for _, m := range ms {
		set.Add(m)
	}
	probe := ms[n/2]
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if ok, _ := set.IsMember(probe); !ok {
			b.Fatal("missing member")
		}
	}
}

func BenchmarkIsMemberSegmented1000(b *testing.B) { benchIsMemberSegmented(b, 1000) }

// --- cell form: the whole set in one store value, the current hybrid path ---

func cellKey() []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, 1)
	return k
}

func encodeCell(ms [][]byte) []byte {
	size := 4
	for _, m := range ms {
		size += 4 + len(m)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf, uint32(len(ms)))
	off := 4
	for _, m := range ms {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m)))
		off += 4
		off += copy(buf[off:], m)
	}
	return buf
}

func decodeCell(buf []byte) [][]byte {
	if len(buf) < 4 {
		return nil
	}
	n := int(binary.BigEndian.Uint32(buf))
	off := 4
	out := make([][]byte, 0, n)
	for range n {
		l := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		out = append(out, buf[off:off+l])
		off += l
	}
	return out
}

func benchAddCell(b *testing.B, n int) {
	ms := members(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		st := newStore(b)
		key := cellKey()
		var have [][]byte
		for _, m := range ms {
			if raw, ok, _ := st.Get(key); ok {
				have = decodeCell(raw)
			}
			have = append(have, m)
			if err := st.Set(key, encodeCell(have)); err != nil {
				b.Fatal(err)
			}
		}
		st.Close()
	}
}

func BenchmarkAddCell100(b *testing.B)  { benchAddCell(b, 100) }
func BenchmarkAddCell1000(b *testing.B) { benchAddCell(b, 1000) }

func benchIsMemberCell(b *testing.B, n int) {
	st := newStore(b)
	defer st.Close()
	ms := members(n)
	st.Set(cellKey(), encodeCell(ms))
	probe := string(ms[n/2])
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		raw, _, _ := st.Get(cellKey())
		found := false
		for _, m := range decodeCell(raw) {
			if string(m) == probe {
				found = true
				break
			}
		}
		if !found {
			b.Fatal("missing member")
		}
	}
}

func BenchmarkIsMemberCell1000(b *testing.B) { benchIsMemberCell(b, 1000) }
