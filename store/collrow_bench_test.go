package store

import (
	"encoding/binary"
	"strconv"
	"testing"
)

// These benchmarks quantify the one decision behind the element-per-row
// collection design (spec 2064 rewrite/03): is a set worth storing as one
// self-contained cell that every SADD rewrites, or as one store row per member?
// The current hybrid collection path takes the cell form, so an element op is
// O(n) in the set size (keyspace/hybrid_coll.go says so in its own header). The
// element-per-row form makes SADD and SISMEMBER O(1) point ops on the store
// index, at the cost of needing a separate enumeration path for SMEMBERS.
//
// Run them to see the gap the redesign is buying, and to keep the claim grounded
// on the target box:
//
//	go test ./store/ -run x -bench BenchmarkColl -benchmem
//
// The numbers are the input to implementation note 285: they say how large a set
// has to get before the cell rewrite is the dominant cost, which is what decides
// whether element-per-row is worth its enumeration complexity.

// collCellKey is the single store key a cell-form set lives under.
func collCellKey(id uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, id)
	return k
}

// encodeCell packs members as [count:4][len:4 member]... the shape an inline cell
// uses: one contiguous value the store holds under the set's key.
func encodeCell(members [][]byte) []byte {
	n := 4
	for _, m := range members {
		n += 4 + len(m)
	}
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf, uint32(len(members)))
	off := 4
	for _, m := range members {
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(m)))
		off += 4
		off += copy(buf[off:], m)
	}
	return buf
}

// decodeCell reverses encodeCell into member slices aliasing the buffer, the work
// a cell-form SADD pays to read the set before appending.
func decodeCell(buf []byte) [][]byte {
	if len(buf) < 4 {
		return nil
	}
	n := int(binary.LittleEndian.Uint32(buf))
	out := make([][]byte, 0, n)
	off := 4
	for i := 0; i < n && off+4 <= len(buf); i++ {
		l := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		out = append(out, buf[off:off+l])
		off += l
	}
	return out
}

// rowKey is the composite store key for one member in the element-per-row form:
// the set id, then the member bytes, so every member is its own point row.
func rowKey(id uint64, member []byte) []byte {
	k := make([]byte, 8+len(member))
	binary.BigEndian.PutUint64(k, id)
	copy(k[8:], member)
	return k
}

func collMembers(n int) [][]byte {
	m := make([][]byte, n)
	for i := range m {
		m[i] = []byte("member:" + strconv.Itoa(i))
	}
	return m
}

// benchSADDCell grows a set to len(members) the cell way: each add reads the whole
// cell, decodes it, appends, re-encodes, and writes it back. This is the O(n) per
// add the current hybrid path pays.
func benchSADDCell(b *testing.B, n int) {
	members := collMembers(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := New(Tunables{Shards: 8, PageSize: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		key := collCellKey(1)
		var have [][]byte
		for _, m := range members {
			if raw, ok, _ := st.Get(key); ok {
				have = decodeCell(raw)
			}
			have = append(have, m)
			if err := st.Set(key, encodeCell(have)); err != nil {
				b.Fatal(err)
			}
		}
		_ = st.Close()
	}
}

// benchSADDRow grows the same set the element-per-row way: each add is one store
// point write under the composite key. O(1) per add.
func benchSADDRow(b *testing.B, n int) {
	members := collMembers(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := New(Tunables{Shards: 8, PageSize: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range members {
			if err := st.Set(rowKey(1, m), nil); err != nil {
				b.Fatal(err)
			}
		}
		_ = st.Close()
	}
}

func BenchmarkCollSADDCell100(b *testing.B)  { benchSADDCell(b, 100) }
func BenchmarkCollSADDRow100(b *testing.B)   { benchSADDRow(b, 100) }
func BenchmarkCollSADDCell1000(b *testing.B) { benchSADDCell(b, 1000) }
func BenchmarkCollSADDRow1000(b *testing.B)  { benchSADDRow(b, 1000) }

// benchSISMEMBERCell answers membership the cell way: read and decode the whole
// cell, then linear-scan for the member. benchSISMEMBERRow does it as one point
// Get on the composite key. This is the read side of the same trade.
func benchSISMEMBERCell(b *testing.B, n int) {
	members := collMembers(n)
	st, _ := New(Tunables{Shards: 8, PageSize: 1 << 20})
	defer st.Close()
	key := collCellKey(1)
	_ = st.Set(key, encodeCell(members))
	probe := members[n/2]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, _, _ := st.Get(key)
		found := false
		for _, m := range decodeCell(raw) {
			if string(m) == string(probe) {
				found = true
				break
			}
		}
		if !found {
			b.Fatal("missing member")
		}
	}
}

func benchSISMEMBERRow(b *testing.B, n int) {
	members := collMembers(n)
	st, _ := New(Tunables{Shards: 8, PageSize: 1 << 20})
	defer st.Close()
	for _, m := range members {
		_ = st.Set(rowKey(1, m), nil)
	}
	probe := members[n/2]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, _ := st.Get(rowKey(1, probe)); !ok {
			b.Fatal("missing member")
		}
	}
}

func BenchmarkCollSISMEMBERCell1000(b *testing.B) { benchSISMEMBERCell(b, 1000) }
func BenchmarkCollSISMEMBERRow1000(b *testing.B)  { benchSISMEMBERRow(b, 1000) }
