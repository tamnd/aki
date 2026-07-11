package list

import (
	"bytes"
	"strconv"
	"testing"
)

// bb turns strings into the [][]byte an argument tail is.
func bb(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// decode reads the whole list into a []string for order assertions.
func decode(l *list) []string {
	var out []string
	l.each(func(v []byte) { out = append(out, string(v)) })
	return out
}

func wantOrder(t *testing.T, l *list, want ...string) {
	t.Helper()
	got := decode(l)
	if len(got) != len(want) {
		t.Fatalf("length %d, want %d (got %v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("elem %d = %q, want %q (got %v)", i, got[i], want[i], got)
		}
	}
	if l.length() != len(want) {
		t.Fatalf("length() = %d, want %d", l.length(), len(want))
	}
}

// A fresh list is an empty listpack band.
func TestNewListBand(t *testing.T) {
	l := newList()
	if l.encoding() != encListpack {
		t.Fatalf("encoding = %s, want listpack", l.encoding())
	}
	if l.encoding().String() != "listpack" {
		t.Fatalf("String = %q, want listpack", l.encoding().String())
	}
	if l.length() != 0 {
		t.Fatalf("length = %d, want 0", l.length())
	}
}

// Pushes land on the right end and pops take from the right end.
func TestPushPopOrder(t *testing.T) {
	l := newList()
	// RPUSH a b c -> [a b c]
	l.pushBack([]byte("a"))
	l.pushBack([]byte("b"))
	l.pushBack([]byte("c"))
	wantOrder(t, l, "a", "b", "c")
	// LPUSH x y -> [y x a b c]
	l.pushFront([]byte("x"))
	l.pushFront([]byte("y"))
	wantOrder(t, l, "y", "x", "a", "b", "c")

	if v := string(l.popFront()); v != "y" {
		t.Fatalf("popFront = %q, want y", v)
	}
	if v := string(l.popBack()); v != "c" {
		t.Fatalf("popBack = %q, want c", v)
	}
	wantOrder(t, l, "x", "a", "b")
}

// A pushFront after pops reuses the dead prefix the head cursor left behind.
func TestPushFrontReusesDeadPrefix(t *testing.T) {
	l := newList()
	for _, s := range []string{"one", "two", "three"} {
		l.pushBack([]byte(s))
	}
	l.popFront() // drops "one", head advances into the blob
	l.pushFront([]byte("z"))
	wantOrder(t, l, "z", "two", "three")
}

// Popping the last element resets the band to empty and reusable.
func TestPopToEmpty(t *testing.T) {
	l := newList()
	l.pushBack([]byte("solo"))
	if v := string(l.popBack()); v != "solo" {
		t.Fatalf("popBack = %q", v)
	}
	if l.length() != 0 {
		t.Fatalf("length = %d, want 0", l.length())
	}
	l.pushBack([]byte("again"))
	wantOrder(t, l, "again")
}

// get walks to a signed-normalized index; the command layer does the folding.
func TestGetAndSet(t *testing.T) {
	l := newList()
	for _, s := range []string{"a", "b", "c", "d"} {
		l.pushBack([]byte(s))
	}
	if v := string(l.get(0)); v != "a" {
		t.Fatalf("get(0) = %q", v)
	}
	if v := string(l.get(3)); v != "d" {
		t.Fatalf("get(3) = %q", v)
	}
	l.setAt(1, []byte("B"))
	wantOrder(t, l, "a", "B", "c", "d")
	// A resize on set: a longer value re-packs the blob.
	l.setAt(2, []byte("cccccccc"))
	wantOrder(t, l, "a", "B", "cccccccc", "d")
}

func TestInsert(t *testing.T) {
	l := newList()
	for _, s := range []string{"a", "b", "c"} {
		l.pushBack([]byte(s))
	}
	if !l.insert(true, []byte("b"), []byte("X")) {
		t.Fatal("insert before b reported missing pivot")
	}
	wantOrder(t, l, "a", "X", "b", "c")
	if !l.insert(false, []byte("c"), []byte("Y")) {
		t.Fatal("insert after c reported missing pivot")
	}
	wantOrder(t, l, "a", "X", "b", "c", "Y")
	if l.insert(true, []byte("nope"), []byte("Z")) {
		t.Fatal("insert reported a match for a missing pivot")
	}
}

// removeMatches is the LREM count-sign rule as a pure function.
func TestRemoveMatches(t *testing.T) {
	base := func() [][]byte { return bb("a", "b", "a", "c", "a") }
	cases := []struct {
		count   int
		want    []string
		removed int
	}{
		{0, []string{"b", "c"}, 3},       // all
		{2, []string{"b", "c", "a"}, 2},  // head to tail
		{-2, []string{"a", "b", "c"}, 2}, // tail to head
		{5, []string{"b", "c"}, 3},       // capped at what exists
		{-5, []string{"b", "c"}, 3},      // same, from the tail
	}
	for _, tc := range cases {
		kept, removed := removeMatches(base(), tc.count, []byte("a"))
		if removed != tc.removed {
			t.Fatalf("count %d: removed %d, want %d", tc.count, removed, tc.removed)
		}
		var got []string
		for _, e := range kept {
			got = append(got, string(e))
		}
		if len(got) != len(tc.want) {
			t.Fatalf("count %d: kept %v, want %v", tc.count, got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("count %d: kept %v, want %v", tc.count, got, tc.want)
			}
		}
	}
}

func TestTrim(t *testing.T) {
	mk := func() *list {
		l := newList()
		for _, s := range []string{"a", "b", "c", "d", "e"} {
			l.pushBack([]byte(s))
		}
		return l
	}
	l := mk()
	l.trim(1, 3)
	wantOrder(t, l, "b", "c", "d")

	l = mk()
	l.trim(1, 0) // empty range clears
	if l.length() != 0 {
		t.Fatalf("empty-range trim left %d", l.length())
	}
}

// normIndex and clampRange fold the signed forms LINDEX and LRANGE take.
func TestIndexFolding(t *testing.T) {
	if got := normIndex(-1, 5); got != 4 {
		t.Fatalf("normIndex(-1,5) = %d, want 4", got)
	}
	if got := normIndex(2, 5); got != 2 {
		t.Fatalf("normIndex(2,5) = %d, want 2", got)
	}
	cases := []struct {
		start, stop, n int
		lo, hi         int
		ok             bool
	}{
		{0, -1, 5, 0, 4, true},
		{-3, -1, 5, 2, 4, true},
		{-100, 2, 5, 0, 2, true},
		{2, 1, 5, 0, 0, false},
		{5, 6, 5, 0, 0, false},
		{0, 0, 0, 0, 0, false},
	}
	for _, tc := range cases {
		lo, hi, ok := clampRange(tc.start, tc.stop, tc.n)
		if ok != tc.ok || (ok && (lo != tc.lo || hi != tc.hi)) {
			t.Fatalf("clampRange(%d,%d,%d) = %d,%d,%v want %d,%d,%v",
				tc.start, tc.stop, tc.n, lo, hi, ok, tc.lo, tc.hi, tc.ok)
		}
	}
}

// lposScan honours RANK direction, COUNT limit, and MAXLEN cap.
func TestLposScan(t *testing.T) {
	l := newList()
	for _, s := range []string{"a", "b", "c", "a", "b", "a"} {
		l.pushBack([]byte(s))
	}
	eq := func(got []int, want ...int) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}
	eq(lposScan(l, []byte("a"), 1, 1, 0), 0)        // first "a"
	eq(lposScan(l, []byte("a"), 2, 1, 0), 3)        // second "a"
	eq(lposScan(l, []byte("a"), -1, 1, 0), 5)       // last "a"
	eq(lposScan(l, []byte("a"), -2, 1, 0), 3)       // second from the end
	eq(lposScan(l, []byte("a"), 1, 0, 0), 0, 3, 5)  // all forward
	eq(lposScan(l, []byte("a"), -1, 0, 0), 5, 3, 0) // all backward
	eq(lposScan(l, []byte("a"), 1, 2, 0), 0, 3)     // COUNT cap
	eq(lposScan(l, []byte("a"), 1, 0, 2), 0)        // MAXLEN 2 compares only [a b]; finds index 0
}

// The inline band promotes to the native placeholder exactly when a write
// crosses the listpack byte budget, and never converts back down.
func TestPromotionAtBudget(t *testing.T) {
	l := newList()
	// Fill with 80-byte strings. The live boundary is 98 in / listpack,
	// 99 in / quicklist (verified against redis-server 8.8.0).
	val := bytes.Repeat([]byte("x"), 80)
	for i := 0; i < 98; i++ {
		l.pushBack(val)
	}
	if l.encoding() != encListpack {
		t.Fatalf("after 98 x80: encoding %s, want listpack (lpsz=%d)", l.encoding(), l.lpsz)
	}
	l.pushBack(val) // the 99th crosses the budget
	if l.encoding() != encQuicklist {
		t.Fatalf("after 99 x80: encoding %s, want quicklist", l.encoding())
	}
	if l.nat == nil {
		t.Fatal("promotion left nat nil")
	}
	if got := l.length(); got != 99 {
		t.Fatalf("length after promotion = %d, want 99", got)
	}
	// Sticky: draining back under the budget keeps quicklist.
	for l.length() > 1 {
		l.popBack()
	}
	if l.encoding() != encQuicklist {
		t.Fatalf("shrunk list reports %s, want sticky quicklist", l.encoding())
	}
}

// A single element at the budget edge promotes; the band stays correct across
// the seam.
func TestPromotionSingleLargeElement(t *testing.T) {
	l := newList()
	big := bytes.Repeat([]byte("y"), 9000) // well past 8 KiB
	l.pushBack(big)
	if l.encoding() != encQuicklist {
		t.Fatalf("9000-byte element: encoding %s, want quicklist", l.encoding())
	}
	if v := string(l.get(0)); v != string(big) {
		t.Fatal("element survived promotion garbled")
	}
}

// lpEntrySize matches the listpack encoder's integer vs string branch and the
// back-length ladder.
func TestEntrySizing(t *testing.T) {
	// A small non-negative int packs into 1 encoding byte + 1 backlen byte.
	if got := lpEntrySize([]byte("5")); got != 2 {
		t.Fatalf("lpEntrySize(5) = %d, want 2", got)
	}
	// A 3-char string: 1 header + 3 data + 1 backlen = 5.
	if got := lpEntrySize([]byte("abc")); got != 5 {
		t.Fatalf("lpEntrySize(abc) = %d, want 5", got)
	}
	// A 200-byte string: 2 header + 200 data + 2 backlen = 204.
	if got := lpEntrySize(bytes.Repeat([]byte("z"), 200)); got != 204 {
		t.Fatalf("lpEntrySize(200z) = %d, want 204", got)
	}
	// A number the size ladder treats as a 3-byte int (fits int16).
	if got := lpIntEncodingSize(30000); got != 3 {
		t.Fatalf("lpIntEncodingSize(30000) = %d, want 3", got)
	}
}

// A numeric string is packed as an integer, so its budget cost is smaller than
// its byte length, the same as Redis.
func TestNumericEntryIsInteger(t *testing.T) {
	// "1234567890" is 10 bytes as a string but fits a 5-byte int32 encoding.
	n := int64(1234567890)
	if _, err := strconv.ParseInt("1234567890", 10, 64); err != nil {
		t.Fatal(err)
	}
	enc := lpIntEncodingSize(n)
	if enc != 5 {
		t.Fatalf("int32 encoding size = %d, want 5", enc)
	}
	if got := lpEntrySize([]byte("1234567890")); got >= 10 {
		t.Fatalf("numeric entry sized as string (%d)", got)
	}
}
