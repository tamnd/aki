package structs

import (
	"encoding/binary"
	"math/bits"
	"strconv"
	"testing"
)

// memberStore is a minimal structs.Set for the table tests: an append-only slab
// of member bytes indexed by ordinal, the same shape the set package's htable
// exposes. hash is a plain FNV so the test does not depend on the engine hash.
type memberStore struct {
	keys [][]byte
}

func (m *memberStore) Match(ord uint32, key []byte) bool {
	a := m.keys[ord]
	if len(a) != len(key) {
		return false
	}
	for i := range a {
		if a[i] != key[i] {
			return false
		}
	}
	return true
}

func (m *memberStore) Rehash(ord uint32) uint64 { return fnv(m.keys[ord]) }

func fnv(b []byte) uint64 {
	h := uint64(1469598103934665603)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// add inserts key if absent, returning its ordinal. It mirrors the SADD probe:
// find, then insert on a miss.
func (m *memberStore) add(t *Table, key []byte) (uint32, bool) {
	h := fnv(key)
	if ord, ok := t.Find(h, key, m); ok {
		return ord, false
	}
	ord := uint32(len(m.keys))
	m.keys = append(m.keys, append([]byte(nil), key...))
	t.Insert(h, ord, m)
	return ord, true
}

func (m *memberStore) has(t *Table, key []byte) bool {
	_, ok := t.Find(fnv(key), key, m)
	return ok
}

func (m *memberStore) del(t *Table, key []byte) bool {
	_, ok := t.Delete(fnv(key), key, m)
	return ok
}

func TestTableAddHasDelete(t *testing.T) {
	tbl := MakeTable(0)
	ms := &memberStore{}
	const n = 5000
	for i := 0; i < n; i++ {
		key := []byte("member:" + strconv.Itoa(i))
		if _, added := ms.add(&tbl, key); !added {
			t.Fatalf("member %d reported already present", i)
		}
	}
	if tbl.Len() != n {
		t.Fatalf("Len = %d, want %d", tbl.Len(), n)
	}
	// Every inserted member is found; a never-inserted one is not.
	for i := 0; i < n; i++ {
		if !ms.has(&tbl, []byte("member:"+strconv.Itoa(i))) {
			t.Fatalf("member %d missing after insert", i)
		}
	}
	if ms.has(&tbl, []byte("member:absent")) {
		t.Fatal("absent member reported present")
	}
	// A duplicate insert is a no-op the count reflects.
	if _, added := ms.add(&tbl, []byte("member:10")); added {
		t.Fatal("duplicate insert reported as new")
	}
	if tbl.Len() != n {
		t.Fatalf("Len after dup = %d, want %d", tbl.Len(), n)
	}
	// Delete half and confirm both halves.
	for i := 0; i < n; i += 2 {
		if !ms.del(&tbl, []byte("member:"+strconv.Itoa(i))) {
			t.Fatalf("delete of member %d reported absent", i)
		}
	}
	if tbl.Len() != n/2 {
		t.Fatalf("Len after deletes = %d, want %d", tbl.Len(), n/2)
	}
	for i := 0; i < n; i++ {
		got := ms.has(&tbl, []byte("member:"+strconv.Itoa(i)))
		want := i%2 == 1
		if got != want {
			t.Fatalf("member %d present=%v, want %v", i, got, want)
		}
	}
}

// TestTableTombstoneReuse pins that delete-then-insert churn does not grow the
// table without bound: the tombstones are reclaimed by the in-place resize, so
// capacity tracks the live set, not the churn count.
func TestTableTombstoneReuse(t *testing.T) {
	tbl := MakeTable(0)
	ms := &memberStore{}
	for i := 0; i < 200; i++ {
		ms.add(&tbl, []byte("k"+strconv.Itoa(i)))
	}
	capAfterFill := tbl.CapSlots()
	// Churn: repeatedly delete and re-add a rotating window, far more ops than
	// the live set size.
	for round := 0; round < 50; round++ {
		for i := 0; i < 200; i++ {
			ms.del(&tbl, []byte("k"+strconv.Itoa(i)))
			ms.add(&tbl, []byte("k"+strconv.Itoa(i)))
		}
	}
	if tbl.Len() != 200 {
		t.Fatalf("Len after churn = %d, want 200", tbl.Len())
	}
	if got := tbl.CapSlots(); got > capAfterFill*2 {
		t.Fatalf("capacity ballooned to %d from %d under churn; tombstones not reclaimed", got, capAfterFill)
	}
	for i := 0; i < 200; i++ {
		if !ms.has(&tbl, []byte("k"+strconv.Itoa(i))) {
			t.Fatalf("member %d lost across churn", i)
		}
	}
}

// TestTableLoadCeiling checks the table never exceeds 7/8 occupancy: after every
// insert the live-plus-dead count stays at or under the ceiling for the current
// capacity.
func TestTableLoadCeiling(t *testing.T) {
	tbl := MakeTable(0)
	ms := &memberStore{}
	for i := 0; i < 20000; i++ {
		ms.add(&tbl, []byte("x"+strconv.Itoa(i)))
		if occ := tbl.count + tbl.dead; occ > tbl.maxLoad() {
			t.Fatalf("occupancy %d over ceiling %d at cap %d", occ, tbl.maxLoad(), tbl.cap)
		}
		if tbl.cap&(tbl.cap-1) != 0 {
			t.Fatalf("capacity %d is not a power of two", tbl.cap)
		}
	}
}

// TestMatchByte is the SWAR primitive's own check: it must flag exactly the byte
// positions equal to the target, for tags, empty, and deleted.
func TestMatchByte(t *testing.T) {
	var buf [8]byte
	buf[0] = 0x11
	buf[1] = ctrlEmpty
	buf[2] = 0x11
	buf[3] = ctrlDeleted
	buf[4] = 0x7f
	buf[5] = 0x00
	buf[6] = 0x11
	buf[7] = ctrlEmpty
	word := binary.LittleEndian.Uint64(buf[:])

	want := func(positions ...int) uint64 {
		var m uint64
		for _, p := range positions {
			m |= 0x80 << (p * 8)
		}
		return m
	}
	if got := matchByte(word, 0x11); got != want(0, 2, 6) {
		t.Errorf("match 0x11 = %#x, want %#x", got, want(0, 2, 6))
	}
	if got := matchByte(word, ctrlEmpty); got != want(1, 7) {
		t.Errorf("match empty = %#x, want %#x", got, want(1, 7))
	}
	if got := matchByte(word, ctrlDeleted); got != want(3) {
		t.Errorf("match deleted = %#x, want %#x", got, want(3))
	}
	if got := matchByte(word, 0x22); got != 0 {
		t.Errorf("match absent tag = %#x, want 0", got)
	}
	// The high-bit isolate that place() uses must flag empty and deleted only.
	if got := word & highBits; got != want(1, 3, 7) {
		t.Errorf("empty-or-deleted = %#x, want %#x", got, want(1, 3, 7))
	}
}

func TestCapFor(t *testing.T) {
	cases := []struct {
		n    int
		want uint32
	}{
		{0, 8}, {1, 8}, {7, 8}, {8, 16}, {14, 16}, {15, 32}, {513, 1024},
	}
	for _, c := range cases {
		if got := capFor(c.n); got != c.want {
			t.Errorf("capFor(%d) = %d, want %d", c.n, got, c.want)
		}
		if got := capFor(c.n); got&(got-1) != 0 {
			t.Errorf("capFor(%d) = %d is not a power of two", c.n, got)
		}
		// The chosen capacity must seat n members under the 7/8 ceiling.
		if uint32(c.n) > capFor(c.n)*loadNum/loadDen {
			t.Errorf("capFor(%d) = %d seats fewer than %d at 7/8", c.n, capFor(c.n), c.n)
		}
	}
	_ = bits.UintSize
}
