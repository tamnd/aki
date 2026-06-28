package collset_test

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/collset"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
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

// Members is the enumeration the design exists for: a bare hash index over the
// members gives O(1) point ops but cannot list one set without scanning the whole
// keyspace. The segmented set walks only its own segments, so SMEMBERS costs set
// size. The benchmark records that number on the real store.
func benchMembersSegmented(b *testing.B, n int) {
	st := newStore(b)
	defer st.Close()
	set := collset.New(st, 1)
	for _, m := range members(n) {
		set.Add(m)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out, err := set.Members()
		if err != nil || len(out) != n {
			b.Fatalf("Members len=%d err=%v", len(out), err)
		}
	}
}

func BenchmarkMembersSegmented1000(b *testing.B) { benchMembersSegmented(b, 1000) }

// Card reads the cardinality straight from the metadata row, no segment touched.
func benchCardSegmented(b *testing.B, n int) {
	st := newStore(b)
	defer st.Close()
	set := collset.New(st, 1)
	for _, m := range members(n) {
		set.Add(m)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if c, _ := set.Card(); c != n {
			b.Fatalf("Card=%d want %d", c, n)
		}
	}
}

func BenchmarkCardSegmented1000(b *testing.B) { benchCardSegmented(b, 1000) }

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

// --- btree coll form: the element-per-row form aki already promotes a large set
// to (one member -> one empty-valued row in a btree sub-tree over the pager).
//
// This is the comparison that decides whether collset is worth wiring in: the
// cell form above is only used for small sets, but a large set already lives in
// this btree coll form, which is also element-per-row and also enumerable. The
// two run on different substrates on purpose: collset's segments sit on the
// hybrid log (the larger-than-memory single file), the btree sits on the paged
// region, so this measures per-op CPU and allocation of each collection backbone,
// not a substrate-controlled A/B. A 4 KB page keeps the btree several levels deep
// at 1000 members, the realistic shape.

func newBtree(b *testing.B) *btree.Tree {
	b.Helper()
	p, err := pager.Create(vfs.NewMem(), "bench.aki", pager.Options{PageSize: 4096})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Close() })
	tr, err := btree.Create(p)
	if err != nil {
		b.Fatal(err)
	}
	return tr
}

func benchAddBtree(b *testing.B, n int) {
	ms := members(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		p, err := pager.Create(vfs.NewMem(), "bench.aki", pager.Options{PageSize: 4096})
		if err != nil {
			b.Fatal(err)
		}
		tr, err := btree.Create(p)
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range ms {
			if err := tr.Put(m, nil); err != nil {
				b.Fatal(err)
			}
		}
		p.Close()
	}
}

func BenchmarkAddBtree1000(b *testing.B) { benchAddBtree(b, 1000) }

func benchIsMemberBtree(b *testing.B, n int) {
	tr := newBtree(b)
	ms := members(n)
	for _, m := range ms {
		if err := tr.Put(m, nil); err != nil {
			b.Fatal(err)
		}
	}
	probe := ms[n/2]
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok, _ := tr.Get(probe); !ok {
			b.Fatal("missing member")
		}
	}
}

func BenchmarkIsMemberBtree1000(b *testing.B) { benchIsMemberBtree(b, 1000) }
