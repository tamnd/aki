package f1raw

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"
)

// collKey builds the same length-prefixed composite element key the hash server uses,
// so the ordered index tests exercise the exact byte layout enumeration relies on.
func collKey(coll, member string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(coll)))
	b := append([]byte{}, tmp[:n]...)
	b = append(b, coll...)
	b = append(b, member...)
	return b
}

// scanAll drains CollScan in bounded batches the way a real HGETALL does and returns
// every composite key under the prefix in order, so a test can assert order and
// completeness without caring about the batch boundary.
func scanAll(s *Store, prefix []byte, batch int) [][]byte {
	var out [][]byte
	var after []byte
	for {
		keys, last := s.CollScan(prefix, after, batch, make([][]byte, 0, batch))
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			out = append(out, append([]byte{}, k...))
		}
		if last == nil {
			break
		}
		after = append([]byte{}, last...)
	}
	return out
}

// The ordered index must enumerate exactly one collection's members, in byte order,
// and never leak a sibling collection whose key shares a prefix by coincidence.
func TestOIndexOrderAndIsolation(t *testing.T) {
	s := New(1<<16, 1<<20)

	// Two collections whose bare keys are prefixes of each other ("h" and "h2") plus a
	// string keyed by the bare collection key, to prove the length prefix and the kind
	// byte keep the three namespaces disjoint.
	insert := func(coll, member, val string) {
		k := collKey(coll, member)
		if _, err := s.PutKind(k, []byte(val), kindTestField); err != nil {
			t.Fatalf("PutKind(%q,%q): %v", coll, member, err)
		}
		s.CollInsert(k, kindTestField)
	}
	insert("h", "b", "2")
	insert("h", "a", "1")
	insert("h", "c", "3")
	insert("h2", "z", "26")
	if err := s.Set([]byte("h"), []byte("bare-string")); err != nil {
		t.Fatal(err)
	}

	prefix := collPrefix("h")
	got := scanAll(s, prefix, 2)
	want := [][]byte{collKey("h", "a"), collKey("h", "b"), collKey("h", "c")}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("key %d: got %q want %q", i, got[i], want[i])
		}
	}
}

// collPrefix mirrors the server's hashPrefix so scanAll can bound one collection.
func collPrefix(coll string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(coll)))
	b := append([]byte{}, tmp[:n]...)
	return append(b, coll...)
}

// kindTestField is a distinct kind for the engine tests so they never alias the
// string namespace by accident.
const kindTestField byte = 0x01

// A removed member must drop out of enumeration, and a value that outgrows its record
// (republished at a new arena offset) must still enumerate with its fresh value, since
// the index node keeps the old offset but the caller re-resolves the value.
func TestOIndexRemoveAndRepublish(t *testing.T) {
	s := New(1<<16, 1<<20)
	put := func(member, val string) {
		k := collKey("h", member)
		created, err := s.PutKind(k, []byte(val), kindTestField)
		if err != nil {
			t.Fatal(err)
		}
		if created {
			s.CollInsert(k, kindTestField)
		}
	}
	put("a", "x")
	put("b", "y")
	put("c", "z")

	// Overgrow b's value so PutKind republishes it at a new offset; created is false so
	// no new index node is made, the old node's offset now trails the live record.
	put("b", "a-much-longer-value-that-will-not-fit-the-original-record-capacity")

	// Delete a, leaving b and c.
	k := collKey("h", "a")
	if !s.DeleteKind(k, kindTestField) {
		t.Fatal("DeleteKind(a) reported absent")
	}
	s.CollRemove(k)

	prefix := collPrefix("h")
	got := scanAll(s, prefix, 8)
	if len(got) != 2 {
		t.Fatalf("got %d keys after delete, want 2: %q", len(got), got)
	}
	if !bytes.Equal(got[0], collKey("h", "b")) || !bytes.Equal(got[1], collKey("h", "c")) {
		t.Fatalf("wrong survivors: %q", got)
	}
	// b's value must be the republished one, re-resolved through the authoritative index.
	v, ok := s.GetKind(collKey("h", "b"), nil, kindTestField)
	if !ok || string(v) != "a-much-longer-value-that-will-not-fit-the-original-record-capacity" {
		t.Fatalf("b resolved to %q ok=%v after republish", v, ok)
	}
}

// A larger population must still come back fully sorted through the batched cursor, so
// the skip list ordering and the prefix bound hold at a size that exercises multiple
// levels and several batch boundaries.
func TestOIndexManyMembersSorted(t *testing.T) {
	s := New(1<<18, 1<<22)
	const n = 5000
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%06d", i)
		k := collKey("big", m)
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	got := scanAll(s, collPrefix("big"), 128)
	if len(got) != n {
		t.Fatalf("got %d, want %d", len(got), n)
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return bytes.Compare(got[i], got[j]) < 0 }) {
		t.Fatal("enumeration not sorted")
	}
}
