package f1raw

import (
	"encoding/binary"
	"testing"
)

// This benchmark is the gate for the order-statistic list migration (spec 2064/f1_rewrite_ltm
// impl/30). Moving the list off the dense-integer-position model fixes the scan-bound commands
// (impl/29) but forces the positional reads (LINDEX, LSET, LRANGE) off their O(1) computed-position
// point read onto an O(log n) order-statistic select, because a list that retires popped keys goes
// sparse and the client index becomes a rank over sparse internal keys. LINDEX passes today at only
// 2.25x at pipeline depth 16, so the question is whether the select stays cheap enough to keep it
// above the 2x bar. This measures the one cost that decides it: a dense computed-position point read
// versus the select-then-arena-read the sparse model would run, on the 2M-element list the bench
// actually probes.
//
// The two paths read the same rows the same number of times; they differ only in how they find the
// row. The dense path computes the position key and does one hash-index GetKind. The sparse path
// does one order-statistic descent to the row's offset (CollSelectOffAt) and one arena read
// (ReadValueAt), no hash probe, because the descent hands back the offset directly. So the sparse
// path trades a hash probe for a skiplist descent plus an arena read; the ratio says whether that
// trade keeps LINDEX over 2x or regresses a passing command.

const listSelN = 2_000_000 // probe-list length, the 2M elements the list bench builds
const listSelVal = 16      // element value width

// buildSelectList inserts listSelN element rows under one list prefix, keyed by the same composite
// order-preserving key shape the list stores (uvarint(len(lkey)) | lkey | 8-byte order key), and
// registers each in the ordered index, so both the computed-position probe and the order-statistic
// select have real rows to reach. It returns the store and the list-key prefix.
func buildSelectList(tb testing.TB) (*Store, []byte) {
	store := New(1<<21, 1<<31)
	lkey := []byte("list:probe")
	var tmp [binary.MaxVarintLen64]byte
	pn := binary.PutUvarint(tmp[:], uint64(len(lkey)))
	prefix := make([]byte, 0, pn+len(lkey))
	prefix = append(prefix, tmp[:pn]...)
	prefix = append(prefix, lkey...)

	var elem [listSelVal]byte
	key := make([]byte, 0, len(prefix)+8)
	for i := 0; i < listSelN; i++ {
		binary.LittleEndian.PutUint32(elem[:4], uint32(i))
		key = append(key[:0], prefix...)
		var ord [8]byte
		binary.BigEndian.PutUint64(ord[:], uint64(int64(i))^(1<<63))
		key = append(key, ord[:]...)
		if _, err := store.PutKind(key, elem[:], 0x05); err != nil {
			tb.Fatal(err)
		}
		store.CollInsert(key, 0x05)
	}
	return store, prefix
}

// posKey rebuilds the dense computed-position key for index i, the O(1) work the dense LINDEX does
// before its one GetKind.
func posKey(dst, prefix []byte, i int) []byte {
	dst = append(dst[:0], prefix...)
	var ord [8]byte
	binary.BigEndian.PutUint64(ord[:], uint64(int64(i))^(1<<63))
	return append(dst, ord[:]...)
}

// BenchmarkListDensePointRead is the LINDEX cost today: compute the position key, one hash-index
// GetKind. It walks a fixed pseudo-random sequence of indices so the reads are scattered the way a
// real LINDEX workload hits the list, not a sequential sweep the prefetcher would hide.
func BenchmarkListDensePointRead(b *testing.B) {
	store, prefix := buildSelectList(b)
	defer store.Close()
	key := make([]byte, 0, len(prefix)+8)
	vbuf := make([]byte, 0, 64)
	idx := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx = idx*6364136223846793005 + 1442695040888963407 // splitmix-style stride, no Date/rand dep
		pos := int(idx % listSelN)
		k := posKey(key, prefix, pos)
		v, ok := store.GetKind(k, vbuf[:0], 0x05)
		if !ok || len(v) != listSelVal {
			b.Fatal("dense read missed")
		}
	}
}

// BenchmarkListSelectRead is the sparse-model LINDEX cost: one order-statistic descent to the row's
// offset, one arena read. Same index sequence as the dense benchmark, so the two numbers are directly
// comparable and their ratio is the LINDEX regression factor the gate turns on.
func BenchmarkListSelectRead(b *testing.B) {
	store, prefix := buildSelectList(b)
	defer store.Close()
	vbuf := make([]byte, 0, 64)
	idx := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx = idx*6364136223846793005 + 1442695040888963407
		pos := int(idx % listSelN)
		off, ok := store.CollSelectOffAt(prefix, pos)
		if !ok {
			b.Fatal("select missed")
		}
		v := store.ReadValueAt(off, vbuf[:0])
		if len(v) != listSelVal {
			b.Fatal("select read wrong length")
		}
	}
}

// BenchmarkListSelectKeyRead is the pessimistic sparse path, kept for contrast: select the key then
// GetKind it, the CollSelectAt-then-probe shape a naive rewire would write, which stacks the hash
// probe on top of the descent. It shows how much the offset-returning select saves versus the
// obvious but slower key-returning one.
func BenchmarkListSelectKeyRead(b *testing.B) {
	store, prefix := buildSelectList(b)
	defer store.Close()
	vbuf := make([]byte, 0, 64)
	idx := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx = idx*6364136223846793005 + 1442695040888963407
		pos := int(idx % listSelN)
		k, ok := store.CollSelectAt(prefix, pos)
		if !ok {
			b.Fatal("select missed")
		}
		v, got := store.GetKind(k, vbuf[:0], 0x05)
		if !got || len(v) != listSelVal {
			b.Fatal("select-key read missed")
		}
	}
}
