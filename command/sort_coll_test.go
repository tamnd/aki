package command

import (
	"bufio"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// scrambled returns 0..n-1 in a fixed non-sorted order, so a list built from it is
// not already in sort order and the SORT under test does real work.
func scrambled(n int) []string {
	out := make([]string, n)
	for i := range n {
		// A stride coprime with n walks every slot once in a scrambled order.
		out[i] = strconv.Itoa((i*7919 + 13) % n)
	}
	return out
}

// sendWords issues an inline command built from words and returns the first reply
// line. The collections here are built from short integer/short-string elements,
// so an inline line carries them without quoting.
func sendWords(t *testing.T, r *bufio.Reader, c net.Conn, words []string) string {
	t.Helper()
	return sendLine(t, r, c, strings.Join(words, " "))
}

// TestSortCollListWindowParity checks the bounded top-K SORT over a coll-form list
// returns exactly what the in-RAM full sort plus LIMIT would: the numeric window,
// the DESC window, a window that runs off the end, and the ALPHA window.
func TestSortCollListWindowParity(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	vals := scrambled(n)
	// Pin the listpack threshold to the 128-entry coll boundary so OBJECT ENCODING
	// reports quicklist once the list crosses into coll form.
	_ = sendLine(t, r, c, "CONFIG SET list-max-listpack-size 128")
	args := append([]string{"RPUSH", "biglist"}, vals...)
	_ = sendWords(t, r, c, args)
	if enc := bulk(t, r, c, "OBJECT ENCODING biglist"); enc != "quicklist" {
		t.Fatalf("list not in coll form: OBJECT ENCODING = %q want quicklist", enc)
	}

	// Numeric ascending and descending references.
	asc := make([]string, n)
	copy(asc, vals)
	sort.Slice(asc, func(i, j int) bool { a, _ := strconv.Atoi(asc[i]); b, _ := strconv.Atoi(asc[j]); return a < b })
	desc := slices.Clone(asc)
	slices.Reverse(desc)
	// Lexical reference for ALPHA.
	lex := slices.Clone(vals)
	sort.Strings(lex)

	cases := []struct {
		cmd  string
		want []string
	}{
		{"SORT biglist LIMIT 0 10", asc[:10]},
		{"SORT biglist LIMIT 5 5", asc[5:10]},
		{"SORT biglist DESC LIMIT 0 10", desc[:10]},
		{"SORT biglist LIMIT 995 100", asc[995:]}, // window runs off the end
		{"SORT biglist LIMIT 2000 10", nil},       // offset past the end
		{"SORT biglist LIMIT 0 0", nil},           // empty window
		{"SORT biglist ALPHA LIMIT 0 5", lex[:5]},
		{"SORT biglist ALPHA DESC LIMIT 0 3", []string{lex[n-1], lex[n-2], lex[n-3]}},
	}
	for _, tc := range cases {
		got := readArray(t, r, c, tc.cmd)
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%q = %v, want %v", tc.cmd, got, tc.want)
		}
	}
}

// TestSortCollSetWindowParity checks the bounded SORT over a coll-form set, which
// has no inherent order so SORT orders it itself.
func TestSortCollSetWindowParity(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	members := make([]string, n)
	for i := range n {
		members[i] = fmt.Sprintf("m:%04d", (i*7919+13)%n)
	}
	args := append([]string{"SADD", "bigset"}, members...)
	_ = sendWords(t, r, c, args)
	if enc := bulk(t, r, c, "OBJECT ENCODING bigset"); enc != "hashtable" {
		t.Fatalf("set not in coll form: OBJECT ENCODING = %q want hashtable", enc)
	}

	lex := slices.Clone(members)
	sort.Strings(lex)
	cases := []struct {
		cmd  string
		want []string
	}{
		{"SORT bigset ALPHA LIMIT 0 5", lex[:5]},
		{"SORT bigset ALPHA LIMIT 2 3", lex[2:5]},
		{"SORT bigset ALPHA DESC LIMIT 0 4", []string{lex[n-1], lex[n-2], lex[n-3], lex[n-4]}},
	}
	for _, tc := range cases {
		got := readArray(t, r, c, tc.cmd)
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%q = %v, want %v", tc.cmd, got, tc.want)
		}
	}
}

// TestSortCollZSetWindowParity checks the bounded SORT over a coll-form sorted set
// orders by the member value, not the score.
func TestSortCollZSetWindowParity(t *testing.T) {
	r, c := startData(t)
	const n = 500
	args := []string{"ZADD", "bigzset"}
	for i := range n {
		m := (i*131 + 7) % n
		// An arbitrary score unrelated to the member, to prove SORT ignores it.
		args = append(args, strconv.Itoa(n-m), strconv.Itoa(m))
	}
	_ = sendWords(t, r, c, args)
	if enc := bulk(t, r, c, "OBJECT ENCODING bigzset"); enc != "skiplist" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q want skiplist", enc)
	}

	asc := make([]string, n)
	for i := range n {
		asc[i] = strconv.Itoa(i)
	}
	sort.Slice(asc, func(i, j int) bool { a, _ := strconv.Atoi(asc[i]); b, _ := strconv.Atoi(asc[j]); return a < b })
	desc := slices.Clone(asc)
	slices.Reverse(desc)

	if got := readArray(t, r, c, "SORT bigzset LIMIT 0 5"); !slices.Equal(got, asc[:5]) {
		t.Fatalf("SORT bigzset LIMIT 0 5 = %v want %v", got, asc[:5])
	}
	if got := readArray(t, r, c, "SORT bigzset DESC LIMIT 0 3"); !slices.Equal(got, desc[:3]) {
		t.Fatalf("SORT bigzset DESC LIMIT 0 3 = %v want %v", got, desc[:3])
	}
}

// TestSortCollWindowByGet checks the bounded path runs the BY weight pattern and
// the GET projection between source pages, so a paginated SORT BY ... GET over a
// coll-form list resolves external keys correctly.
func TestSortCollWindowByGet(t *testing.T) {
	r, c := startData(t)
	const n = 300
	ids := make([]string, n)
	for i := range n {
		id := i + 1
		ids[i] = strconv.Itoa(id)
		// Weight is the id itself, so ascending BY weight orders by id; data key is a
		// distinct payload to prove GET dereferences the resolved key.
		_ = sendLine(t, r, c, fmt.Sprintf("SET w_%d %d", id, id))
		_ = sendLine(t, r, c, fmt.Sprintf("SET d_%d D%d", id, id))
	}
	// Pin the listpack threshold to the 128-entry coll boundary so OBJECT ENCODING
	// reports quicklist once the list crosses into coll form.
	_ = sendLine(t, r, c, "CONFIG SET list-max-listpack-size 128")
	args := append([]string{"RPUSH", "ids"}, ids...)
	_ = sendWords(t, r, c, args)
	if enc := bulk(t, r, c, "OBJECT ENCODING ids"); enc != "quicklist" {
		t.Fatalf("list not in coll form: OBJECT ENCODING = %q", enc)
	}

	// The two smallest weights are ids 1 and 2; GET d_* then GET # expands each.
	got := readArray(t, r, c, "SORT ids BY w_* GET d_* GET # LIMIT 0 2")
	want := []string{"D1", "1", "D2", "2"}
	if !slices.Equal(got, want) {
		t.Fatalf("SORT BY GET window = %v want %v", got, want)
	}
}

// TestSortCollWindowConvOutsideWindow checks a numeric SORT is rejected when any
// element is non-numeric, even when that element falls outside the LIMIT window,
// matching the in-RAM path which sorts every element before applying the window.
func TestSortCollWindowConvOutsideWindow(t *testing.T) {
	r, c := startData(t)
	const n = 600
	vals := make([]string, 0, n+1)
	for i := range n {
		vals = append(vals, strconv.Itoa(i))
	}
	// A non-numeric element late in the list, well past a small window.
	vals = append(vals, "notanumber")
	// Pin the listpack threshold to the 128-entry coll boundary so OBJECT ENCODING
	// reports quicklist once the list crosses into coll form.
	_ = sendLine(t, r, c, "CONFIG SET list-max-listpack-size 128")
	args := append([]string{"RPUSH", "mixed"}, vals...)
	_ = sendWords(t, r, c, args)
	if enc := bulk(t, r, c, "OBJECT ENCODING mixed"); enc != "quicklist" {
		t.Fatalf("list not in coll form: OBJECT ENCODING = %q", enc)
	}
	if got := sendLine(t, r, c, "SORT mixed LIMIT 0 5"); got != "-ERR One or more scores can't be converted into double" {
		t.Fatalf("SORT with a non-numeric element outside the window = %q", got)
	}
}

// TestSortCollWindowStore checks the bounded path under STORE writes the window to
// the destination list.
func TestSortCollWindowStore(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	vals := scrambled(n)
	args := append([]string{"RPUSH", "src"}, vals...)
	_ = sendWords(t, r, c, args)

	if got := sendLine(t, r, c, "SORT src LIMIT 0 10 STORE dst"); got != ":10" {
		t.Fatalf("SORT ... STORE = %q want :10", got)
	}
	got := readArray(t, r, c, "LRANGE dst 0 -1")
	want := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	if !slices.Equal(got, want) {
		t.Fatalf("STORE dst = %v want %v", got, want)
	}
}

// TestSortCollFullSortFallback checks a coll-form SORT with no LIMIT still returns
// the whole sorted collection through the in-RAM fallback the window path defers
// to, so the change does not regress the unbounded case.
func TestSortCollFullSortFallback(t *testing.T) {
	r, c := startData(t)
	const n = 500
	vals := scrambled(n)
	args := append([]string{"RPUSH", "full"}, vals...)
	_ = sendWords(t, r, c, args)

	asc := make([]string, n)
	copy(asc, vals)
	sort.Slice(asc, func(i, j int) bool { a, _ := strconv.Atoi(asc[i]); b, _ := strconv.Atoi(asc[j]); return a < b })
	got := readArray(t, r, c, "SORT full")
	if !slices.Equal(got, asc) {
		t.Fatalf("SORT full (no limit) returned %d elements, mismatch with the full sort", len(got))
	}
}

// TestSortCollWindowIsBounded is the OOM witness: a paginated SORT over a coll-form
// list far larger than its LIMIT window must allocate on the order of the window,
// not the list. The old path called getList, cloning every element onto the heap
// before sorting; the bounded path streams the elements in pages and keeps only
// the top offset+count in a heap. The per-call allocation must not grow when the
// list doubles.
func TestSortCollWindowIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	build := func(n int) (*Dispatcher, *networking.Conn) {
		d := newFuzzDispatcher(t)
		conn := networking.NewOfflineConn()
		args := [][]byte{[]byte("RPUSH"), []byte("k")}
		for i := range n {
			args = append(args, []byte(strconv.Itoa((i*7919+13)%n)))
		}
		conn.ResetOut()
		d.Handle(conn, args)
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("k")})
		if got := string(conn.OutBytes()); got != "$9\r\nquicklist\r\n" {
			t.Fatalf("list not in coll form: OBJECT ENCODING = %q", got)
		}
		return d, conn
	}
	sortArgs := [][]byte{[]byte("SORT"), []byte("k"), []byte("LIMIT"), []byte("0"), []byte("10")}
	measure := func(d *Dispatcher, conn *networking.Conn) float64 {
		return testing.AllocsPerRun(10, func() {
			conn.ResetOut()
			d.Handle(conn, sortArgs)
		})
	}

	d1, c1 := build(20000)
	small := measure(d1, c1)
	d2, c2 := build(40000)
	large := measure(d2, c2)

	// A materialize of n elements would allocate tens of thousands of objects; a
	// window of 10 touches a bounded number of pages plus the reply.
	if small > 1500 {
		t.Fatalf("SORT LIMIT 0 10 over 20000 elements allocated %.0f objects per run; "+
			"the window path should track the LIMIT, not the list size", small)
	}
	// Doubling the list must not grow the per-call allocation.
	if large > small*2+300 {
		t.Fatalf("SORT LIMIT 0 10 allocated %.0f over 40000 elements vs %.0f over 20000; "+
			"per-call cost must not scale with the collection", large, small)
	}
}
