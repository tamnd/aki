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

// CollSelectAt must return the localIndex-th member of one collection in key order,
// isolated from sibling collections that share a bounding prefix, and must report absent
// for a localIndex at or past the collection's cardinality rather than leaking a sibling.
func TestOIndexSelectAt(t *testing.T) {
	s := New(1<<16, 1<<20)
	insert := func(coll, member string) {
		k := collKey(coll, member)
		if _, err := s.PutKind(k, nil, kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	// Two collections whose bare keys are prefixes of one another, plus one whose bare
	// key sorts before both, so a wrong base rank or a missing prefix guard would show.
	insert("a", "0")
	for _, m := range []string{"c", "a", "e", "b", "d"} {
		insert("h", m)
	}
	insert("h2", "z")

	prefix := collPrefix("h")
	want := []string{"a", "b", "c", "d", "e"} // sorted member order within "h"
	for i, w := range want {
		k, ok := s.CollSelectAt(prefix, i)
		if !ok {
			t.Fatalf("CollSelectAt(h, %d) absent, want %q", i, w)
		}
		if got := string(k[len(prefix):]); got != w {
			t.Fatalf("CollSelectAt(h, %d) = %q, want %q", i, got, w)
		}
	}
	// One past the cardinality is absent, not "h2"'s member.
	if k, ok := s.CollSelectAt(prefix, len(want)); ok {
		t.Fatalf("CollSelectAt(h, %d) = %q, want absent", len(want), k[len(prefix):])
	}
	if _, ok := s.CollSelectAt(prefix, -1); ok {
		t.Fatal("CollSelectAt(h, -1) reported present")
	}
}

// The order-statistic spans must stay exact through interleaved inserts and deletes:
// after every mutation, CollSelectAt over the full index must agree element-for-element
// with a plain sorted enumeration, which only holds if every width is maintained right.
func TestOIndexSelectAtAfterMutation(t *testing.T) {
	s := New(1<<18, 1<<22)
	live := map[string]bool{}
	prefix := collPrefix("s")

	add := func(m string) {
		k := collKey("s", m)
		created, err := s.PutKind(k, nil, kindTestField)
		if err != nil {
			t.Fatal(err)
		}
		if created {
			s.CollInsert(k, kindTestField)
		}
		live[m] = true
	}
	del := func(m string) {
		k := collKey("s", m)
		if s.DeleteKind(k, kindTestField) {
			s.CollRemove(k)
		}
		delete(live, m)
	}

	for i := 0; i < 400; i++ {
		add(fmt.Sprintf("m%04d", i))
	}
	// Punch holes across the population so several skip levels re-bridge their spans.
	for i := 0; i < 400; i += 3 {
		del(fmt.Sprintf("m%04d", i))
	}
	for i := 400; i < 500; i++ {
		add(fmt.Sprintf("m%04d", i))
	}

	// Ground truth: the sorted live members.
	var wantMembers []string
	for m := range live {
		wantMembers = append(wantMembers, m)
	}
	sort.Strings(wantMembers)

	if got := len(scanAll(s, prefix, 64)); got != len(wantMembers) {
		t.Fatalf("scan sees %d members, want %d", got, len(wantMembers))
	}
	for i, w := range wantMembers {
		k, ok := s.CollSelectAt(prefix, i)
		if !ok {
			t.Fatalf("CollSelectAt(%d) absent, want %q", i, w)
		}
		if got := string(k[len(prefix):]); got != w {
			t.Fatalf("CollSelectAt(%d) = %q, want %q", i, got, w)
		}
	}
	if _, ok := s.CollSelectAt(prefix, len(wantMembers)); ok {
		t.Fatalf("CollSelectAt(%d) present past cardinality", len(wantMembers))
	}
}

// A uniform localIndex draw must reach every member with roughly equal frequency, which
// is the property SPOP/SRANDMEMBER rely on: order-statistic selection is exactly uniform
// over the index, unlike a byte-space random seek that clumps on the member distribution.
func TestOIndexSelectAtUniformCoverage(t *testing.T) {
	s := New(1<<16, 1<<20)
	const n = 64
	for i := 0; i < n; i++ {
		k := collKey("u", fmt.Sprintf("m%03d", i))
		if _, err := s.PutKind(k, nil, kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	prefix := collPrefix("u")
	seen := make([]int, n)
	// A fixed splitmix walk over [0,n) stands in for the server's uniform draw, so the
	// test is deterministic while still touching every index.
	var r uint64 = 0x1234567
	for i := 0; i < n*50; i++ {
		r += 0x9e3779b97f4a7c15
		z := r
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		idx := int(z % n)
		k, ok := s.CollSelectAt(prefix, idx)
		if !ok {
			t.Fatalf("CollSelectAt(%d) absent", idx)
		}
		pos := int(k[len(prefix)+1]-'0')*100 + int(k[len(prefix)+2]-'0')*10 + int(k[len(prefix)+3]-'0')
		if pos != idx {
			t.Fatalf("index %d resolved to member m%03d", idx, pos)
		}
		seen[idx]++
	}
	for i, c := range seen {
		if c == 0 {
			t.Fatalf("index %d never selected", i)
		}
	}
}
