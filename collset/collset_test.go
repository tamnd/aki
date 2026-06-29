package collset

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// memKV is an in-memory KV so the structure is tested with no engine present. It
// copies on Set so a stored row never aliases a caller buffer, matching the real
// store's contract.
type memKV struct {
	m map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (k *memKV) Get(key []byte) ([]byte, bool, error) {
	v, ok := k.m[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (k *memKV) Set(key, value []byte) error {
	k.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (k *memKV) Delete(key []byte) (bool, error) {
	_, ok := k.m[string(key)]
	delete(k.m, string(key))
	return ok, nil
}

func (k *memKV) rows() int { return len(k.m) }

func TestSetAddIsMemberRemove(t *testing.T) {
	s := New(newMemKV(), 7)

	if ok, _ := s.IsMember([]byte("x")); ok {
		t.Fatal("empty set reports a member")
	}
	if n, _ := s.Card(); n != 0 {
		t.Fatalf("empty Card = %d, want 0", n)
	}

	added, err := s.Add([]byte("b"))
	if err != nil || !added {
		t.Fatalf("Add b: added=%v err=%v", added, err)
	}
	if added, _ := s.Add([]byte("b")); added {
		t.Fatal("re-Add b reported newly added")
	}
	s.Add([]byte("a"))
	s.Add([]byte("c"))

	for _, m := range []string{"a", "b", "c"} {
		if ok, _ := s.IsMember([]byte(m)); !ok {
			t.Fatalf("IsMember %q = false", m)
		}
	}
	if ok, _ := s.IsMember([]byte("z")); ok {
		t.Fatal("IsMember z = true")
	}
	if n, _ := s.Card(); n != 3 {
		t.Fatalf("Card = %d, want 3", n)
	}

	mem, _ := s.Members()
	if got := joinSorted(mem); got != "a,b,c" {
		t.Fatalf("Members = %q, want a,b,c", got)
	}

	if removed, _ := s.Remove([]byte("b")); !removed {
		t.Fatal("Remove b = false")
	}
	if removed, _ := s.Remove([]byte("b")); removed {
		t.Fatal("re-Remove b = true")
	}
	if n, _ := s.Card(); n != 2 {
		t.Fatalf("Card after remove = %d, want 2", n)
	}
}

// TestSetSplitAndReference drives enough adds to force many segment splits, then
// checks every operation against a reference set, including the post-split member
// order and the empty-set cleanup.
func TestSetSplitAndReference(t *testing.T) {
	kv := newMemKV()
	s := New(kv, 99)
	ref := map[string]bool{}

	// A deterministic, spread-out key order so segments split and routing is
	// exercised across boundaries (no Math.random, which the environment forbids).
	const n = 5000
	x := uint32(2166136261)
	keys := make([]string, n)
	for i := range keys {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		keys[i] = "k:" + strconv.FormatUint(uint64(x), 16) + ":" + strconv.Itoa(i)
	}

	for _, k := range keys {
		added, err := s.Add([]byte(k))
		if err != nil {
			t.Fatalf("Add %q: %v", k, err)
		}
		if added == ref[k] {
			t.Fatalf("Add %q added=%v but ref present=%v", k, added, ref[k])
		}
		ref[k] = true
	}

	if n2, _ := s.Card(); n2 != len(ref) {
		t.Fatalf("Card = %d, want %d", n2, len(ref))
	}

	// Every reference member is present; a non-member is not.
	for k := range ref {
		if ok, _ := s.IsMember([]byte(k)); !ok {
			t.Fatalf("IsMember %q = false after splits", k)
		}
	}
	if ok, _ := s.IsMember([]byte("absent")); ok {
		t.Fatal("IsMember absent = true")
	}

	// Members are globally sorted across all segments with no duplicates.
	mem, _ := s.Members()
	if len(mem) != len(ref) {
		t.Fatalf("Members len = %d, want %d", len(mem), len(ref))
	}
	for i := 1; i < len(mem); i++ {
		if bytes.Compare(mem[i-1], mem[i]) >= 0 {
			t.Fatalf("Members not strictly sorted at %d: %q then %q", i, mem[i-1], mem[i])
		}
	}

	// Remove everything; the set must end with no rows at all.
	for _, k := range keys {
		if removed, _ := s.Remove([]byte(k)); !removed {
			t.Fatalf("Remove %q = false", k)
		}
	}
	if n2, _ := s.Card(); n2 != 0 {
		t.Fatalf("Card after draining = %d, want 0", n2)
	}
	if kv.rows() != 0 {
		t.Fatalf("drained set left %d rows behind, want 0", kv.rows())
	}
}

func joinSorted(members [][]byte) string {
	ss := make([]string, len(members))
	for i, m := range members {
		ss[i] = string(m)
	}
	sort.Strings(ss)
	return strings.Join(ss, ",")
}
