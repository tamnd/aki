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

// scanAllKV drains CollScanKV the way HGETALL/HVALS do and returns each composite key
// paired with the value read straight from the offset the ordered index yielded, so a
// test can assert the fused value-carrying walk agrees with the point path.
func scanAllKV(s *Store, prefix []byte, batch int) (keys [][]byte, vals [][]byte) {
	var after []byte
	for {
		ks, offs, last := s.CollScanKV(prefix, after, batch, make([][]byte, 0, batch), make([]uint64, 0, batch))
		if len(ks) == 0 {
			break
		}
		for i, k := range ks {
			keys = append(keys, append([]byte{}, k...))
			vals = append(vals, append([]byte{}, s.ReadValueAt(offs[i], nil)...))
		}
		if last == nil {
			break
		}
		after = append([]byte{}, last...)
	}
	return keys, vals
}

// The value-carrying scan must return, for every member, the same value a point GetKind
// returns, in key order, across batch boundaries.
func TestOIndexScanKVMatchesPoint(t *testing.T) {
	s := New(1<<16, 1<<20)
	members := map[string]string{
		"alpha": "one", "bravo": "two", "charlie": "three",
		"delta": "four", "echo": "five", "foxtrot": "six",
	}
	for m, v := range members {
		k := collKey("h", m)
		if _, err := s.PutKind(k, []byte(v), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	// Batch of 2 forces several resume boundaries over six members.
	keys, vals := scanAllKV(s, collPrefix("h"), 2)
	if len(keys) != len(members) {
		t.Fatalf("got %d members, want %d", len(keys), len(members))
	}
	prev := ""
	for i, k := range keys {
		if ks := string(k); ks <= prev {
			t.Fatalf("keys out of order at %d: %q then %q", i, prev, ks)
		} else {
			prev = ks
		}
		pv, ok := s.GetKind(k, nil, kindTestField)
		if !ok {
			t.Fatalf("point GetKind absent for %q", k)
		}
		if !bytes.Equal(vals[i], pv) {
			t.Fatalf("member %q: scan value %q, point value %q", k, vals[i], pv)
		}
	}
}

// The staleness guard the fused path depends on: a value that outgrows its record is
// republished at a new offset, and CollScanKV must read the fresh value from the node, not
// the abandoned old record. Without PutKind refreshing the ordered node's offset on the
// outgrow, ReadValueAt would return the stale value. This is the whole reason the offset
// can be trusted as the value source.
func TestOIndexScanKVRepublishFresh(t *testing.T) {
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
	put("a", "short-a")
	put("b", "short-b")
	put("c", "short-c")

	// Outgrow b so PutKind republishes it at a new offset. created is false, so the server
	// makes no new node; the fix is that PutKind refreshes the existing node to the new
	// offset, which is what keeps the fused scan honest.
	big := "b-value-far-too-long-to-fit-the-original-eight-byte-rounded-capacity-of-short-b"
	put("b", big)

	keys, vals := scanAllKV(s, collPrefix("h"), 8)
	if len(keys) != 3 {
		t.Fatalf("got %d members, want 3: %q", len(keys), keys)
	}
	got := map[string]string{}
	for i, k := range keys {
		got[string(k[len(collPrefix("h")):])] = string(vals[i])
	}
	if got["b"] != big {
		t.Fatalf("b scanned as %q, want the republished value %q", got["b"], big)
	}
	if got["a"] != "short-a" || got["c"] != "short-c" {
		t.Fatalf("a=%q c=%q, want short-a/short-c", got["a"], got["c"])
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

// CollSelectRemoveAt is the fused select-then-remove SPOP runs: it must return the
// localIndex-th member of one collection and unlink it in the same descent, keeping every
// order-statistic width exact so a following full enumeration still matches the remaining
// sorted members. Draining the whole collection one random position at a time is the
// adversarial case: if the fused unlink miscounts a single span, a later select lands on
// the wrong member or reports a live member absent. Sibling collections that bracket the
// drained one must stay intact, and an out-of-range index must remove nothing.
func TestOIndexSelectRemoveAt(t *testing.T) {
	s := New(1<<18, 1<<22)
	insert := func(coll, member string) {
		k := collKey(coll, member)
		if _, err := s.PutKind(k, nil, kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	// Sibling collections whose bare keys bracket "h" on both sides, so a base-rank or
	// prefix-guard slip in the fused path would splice a neighbour's node.
	insert("a", "0")
	live := map[string]bool{}
	for i := 0; i < 300; i++ {
		m := fmt.Sprintf("m%04d", i)
		insert("h", m)
		live[m] = true
	}
	insert("h2", "z")
	prefix := collPrefix("h")

	// A fixed splitmix walk stands in for the server's uniform draw so the drain is
	// deterministic. Remove the (r mod card)-th live member each step, verify it was live,
	// then check the full remaining enumeration equals the sorted live set.
	var r uint64 = 0xabcdef
	for len(live) > 0 {
		card := len(live)
		r += 0x9e3779b97f4a7c15
		z := r
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z ^= z >> 31
		idx := int(z % uint64(card))
		k, ok := s.CollSelectRemoveAt(prefix, idx)
		if !ok {
			t.Fatalf("CollSelectRemoveAt(%d) absent at card %d", idx, card)
		}
		m := string(k[len(prefix):])
		if !live[m] {
			t.Fatalf("removed %q which was not live", m)
		}
		// The removed member must have been the idx-th in sorted order.
		var sorted []string
		for lm := range live {
			sorted = append(sorted, lm)
		}
		sort.Strings(sorted)
		if sorted[idx] != m {
			t.Fatalf("index %d removed %q, want %q", idx, m, sorted[idx])
		}
		// Drop the resident row too, matching how SPOP pairs DeleteKind with the unlink.
		s.DeleteKind(k, kindTestField)
		delete(live, m)

		// Full enumeration must still match the remaining sorted live set exactly.
		sorted = sorted[:0]
		for lm := range live {
			sorted = append(sorted, lm)
		}
		sort.Strings(sorted)
		for i, w := range sorted {
			got, ok := s.CollSelectAt(prefix, i)
			if !ok {
				t.Fatalf("after removing %q, CollSelectAt(%d) absent, want %q", m, i, w)
			}
			if g := string(got[len(prefix):]); g != w {
				t.Fatalf("after removing %q, CollSelectAt(%d) = %q, want %q", m, i, g, w)
			}
		}
		if _, ok := s.CollSelectAt(prefix, len(sorted)); ok {
			t.Fatalf("CollSelectAt(%d) present past cardinality after removing %q", len(sorted), m)
		}
	}
	// Draining "h" must not have touched its bracketing siblings.
	if k, ok := s.CollSelectAt(collPrefix("a"), 0); !ok || string(k[len(collPrefix("a")):]) != "0" {
		t.Fatal("sibling collection a lost its member")
	}
	if k, ok := s.CollSelectAt(collPrefix("h2"), 0); !ok || string(k[len(collPrefix("h2")):]) != "z" {
		t.Fatal("sibling collection h2 lost its member")
	}
	// An out-of-range index on the now-empty collection removes nothing.
	if _, ok := s.CollSelectRemoveAt(prefix, 0); ok {
		t.Fatal("CollSelectRemoveAt on empty collection reported present")
	}
	if _, ok := s.CollSelectRemoveAt(prefix, -1); ok {
		t.Fatal("CollSelectRemoveAt(-1) reported present")
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
