package set

import (
	"sort"
	"strconv"
	"testing"
)

// members drains a set through each into a fresh sorted slice, so a test can
// compare contents without depending on the encoding's iteration order.
func members(s *set) []string {
	var out []string
	s.each(func(m []byte) { out = append(out, string(m)) })
	sort.Strings(out)
	return out
}

func TestAddHasRemIntset(t *testing.T) {
	s := newSet([]byte("10"))
	if s.enc != encIntset {
		t.Fatalf("integer first member should open an intset, got %s", s.enc)
	}
	for _, m := range []string{"10", "3", "3", "7", "-1"} {
		s.add([]byte(m))
	}
	if got := s.card(); got != 4 {
		t.Fatalf("card = %d, want 4 (one duplicate dropped)", got)
	}
	for _, m := range []string{"-1", "3", "7", "10"} {
		if !s.has([]byte(m)) {
			t.Fatalf("has(%q) = false, want true", m)
		}
	}
	if s.has([]byte("11")) {
		t.Fatal("has(11) = true, want false")
	}
	if s.has([]byte("notint")) {
		t.Fatal("has(notint) on an intset must be false")
	}
	got := members(s)
	want := map[string]bool{"-1": true, "3": true, "7": true, "10": true}
	if len(got) != len(want) {
		t.Fatalf("members = %v, want the four -1,3,7,10", got)
	}
	for _, m := range got {
		if !want[m] {
			t.Fatalf("members = %v, unexpected %q", got, m)
		}
	}
	if !s.rem([]byte("3")) || s.rem([]byte("3")) {
		t.Fatal("rem(3) should report true once then false")
	}
	if s.card() != 3 {
		t.Fatalf("card after rem = %d, want 3", s.card())
	}
}

func TestAddHasRemListpack(t *testing.T) {
	s := newSet([]byte("apple"))
	if s.enc != encListpack {
		t.Fatalf("non-integer first member should open a listpack, got %s", s.enc)
	}
	for _, m := range []string{"apple", "apple", "banana", "cherry", ""} {
		s.add([]byte(m))
	}
	if got := s.card(); got != 4 {
		t.Fatalf("card = %d, want 4 (apple once, plus empty member)", got)
	}
	for _, m := range []string{"apple", "banana", "cherry", ""} {
		if !s.has([]byte(m)) {
			t.Fatalf("has(%q) = false, want true", m)
		}
	}
	if s.has([]byte("durian")) {
		t.Fatal("has(durian) = true, want false")
	}
	if !s.rem([]byte("banana")) {
		t.Fatal("rem(banana) = false, want true")
	}
	if s.has([]byte("banana")) {
		t.Fatal("banana still present after rem")
	}
	// The remaining members survive the mid-blob splice.
	for _, m := range []string{"apple", "cherry", ""} {
		if !s.has([]byte(m)) {
			t.Fatalf("has(%q) = false after removing banana", m)
		}
	}
}

// A non-integer member added to an intset converts it to a listpack while the
// result still fits both listpack caps, and the integer members survive as
// their decimal strings.
func TestIntsetToListpackOnNonInt(t *testing.T) {
	s := newSet([]byte("1"))
	s.add([]byte("1"))
	s.add([]byte("2"))
	s.add([]byte("3"))
	if s.enc != encIntset {
		t.Fatalf("still all integers, want intset, got %s", s.enc)
	}
	s.add([]byte("hello"))
	if s.enc != encListpack {
		t.Fatalf("non-integer member should convert to listpack, got %s", s.enc)
	}
	for _, m := range []string{"1", "2", "3", "hello"} {
		if !s.has([]byte(m)) {
			t.Fatalf("has(%q) = false after conversion", m)
		}
	}
}

// Breaching the intset cap lands straight in the hashtable placeholder, because
// the intset cap (512) sits well above the listpack entry cap (128).
func TestIntsetBreachToHashtable(t *testing.T) {
	s := newSet([]byte("0"))
	for i := 0; i < maxIntsetEntries; i++ {
		s.add([]byte(strconv.Itoa(i)))
	}
	if s.enc != encIntset || s.card() != maxIntsetEntries {
		t.Fatalf("at cap: enc=%s card=%d, want intset %d", s.enc, s.card(), maxIntsetEntries)
	}
	s.add([]byte(strconv.Itoa(maxIntsetEntries)))
	if s.enc != encHashtable {
		t.Fatalf("breaching intset cap should convert to hashtable, got %s", s.enc)
	}
	if s.card() != maxIntsetEntries+1 {
		t.Fatalf("card after breach = %d, want %d", s.card(), maxIntsetEntries+1)
	}
	if !s.has([]byte("0")) || !s.has([]byte(strconv.Itoa(maxIntsetEntries))) {
		t.Fatal("members lost across the hashtable conversion")
	}
}

// Breaching a listpack cap (entries or value width) converts to the hashtable
// placeholder; removal afterward never converts back (F4, one-way).
func TestListpackBreachAndOneWay(t *testing.T) {
	s := newSet([]byte("m0"))
	for i := 0; i < maxListpackEntries; i++ {
		s.add([]byte("m" + strconv.Itoa(i)))
	}
	if s.enc != encListpack {
		t.Fatalf("at entry cap: enc=%s, want listpack", s.enc)
	}
	s.add([]byte("one-more"))
	if s.enc != encHashtable {
		t.Fatalf("breaching listpack entry cap should convert, got %s", s.enc)
	}
	// Shrinking below the cap keeps the hashtable encoding.
	for i := 0; i < maxListpackEntries; i++ {
		s.rem([]byte("m" + strconv.Itoa(i)))
	}
	if s.enc != encHashtable {
		t.Fatalf("removal must never convert back, got %s", s.enc)
	}
}

func TestListpackValueBreach(t *testing.T) {
	s := newSet([]byte("short"))
	s.add([]byte("short"))
	big := make([]byte, maxListpackValue) // exactly at the limit: stays listpack
	for i := range big {
		big[i] = 'a'
	}
	s.add(big)
	if s.enc != encListpack {
		t.Fatalf("a %d-byte member is at the limit, want listpack, got %s", maxListpackValue, s.enc)
	}
	over := make([]byte, maxListpackValue+1) // one over: converts
	for i := range over {
		over[i] = 'b'
	}
	s.add(over)
	if s.enc != encHashtable {
		t.Fatalf("a %d-byte member should convert, got %s", maxListpackValue+1, s.enc)
	}
	if !s.has(over) || !s.has(big) {
		t.Fatal("wide members lost across the conversion")
	}
}

// at draws every index exactly once across a full sweep, so SPOP and
// SRANDMEMBER draw the whole set with no gaps or repeats.
func TestAtCoversEveryIndex(t *testing.T) {
	for _, enc := range []string{"intset", "listpack"} {
		var s *set
		want := map[string]bool{}
		if enc == "intset" {
			s = newSet([]byte("0"))
			for i := 0; i < 20; i++ {
				s.add([]byte(strconv.Itoa(i)))
				want[strconv.Itoa(i)] = true
			}
		} else {
			s = newSet([]byte("k0"))
			for i := 0; i < 20; i++ {
				s.add([]byte("k" + strconv.Itoa(i)))
				want["k"+strconv.Itoa(i)] = true
			}
		}
		got := map[string]bool{}
		var sc [64]byte
		for i := 0; i < s.card(); i++ {
			got[string(s.at(i, sc[:]))] = true
		}
		if len(got) != len(want) {
			t.Fatalf("%s: at covered %d distinct members, want %d", enc, len(got), len(want))
		}
		for m := range want {
			if !got[m] {
				t.Fatalf("%s: at never returned %q", enc, m)
			}
		}
	}
}

// The owner-local PRNG spreads draws roughly uniformly; a coarse chi-style
// bucket check catches a stuck or degenerate generator without being flaky.
func TestDrawSpread(t *testing.T) {
	g := &reg{rng: 0x9e3779b97f4a7c15}
	const n, iters = 8, 80000
	var hits [n]int
	for i := 0; i < iters; i++ {
		hits[g.next(n)]++
	}
	exp := iters / n
	for i, h := range hits {
		if h < exp*7/10 || h > exp*13/10 {
			t.Fatalf("bucket %d got %d draws, expected near %d", i, h, exp)
		}
	}
}
