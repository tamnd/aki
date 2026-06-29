package command

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	"github.com/tamnd/aki/networking"
)

// buildCollList RPUSHes elems onto key and asserts the list ended up in coll form,
// the precondition for exercising the streamed list-modify paths.
func buildCollList(t *testing.T, r *bufio.Reader, c net.Conn, eng *Engine, key string, elems []string) {
	t.Helper()
	for i, e := range elems {
		want := fmt.Sprintf(":%d", i+1)
		if got := sendLine(t, r, c, "RPUSH "+key+" "+e); got != want {
			t.Fatalf("RPUSH %s %q = %q want %q", key, e, got, want)
		}
	}
	if !listIsColl(t, eng, key) {
		t.Fatalf("%d-element list %q should be stored coll", len(elems), key)
	}
}

// collListContents reads the whole coll list back with LRANGE so a test can
// compare the streamed mutation against the model it expects.
func collListContents(t *testing.T, r *bufio.Reader, c net.Conn, key string) []string {
	t.Helper()
	return readArray(t, r, c, "LRANGE "+key+" 0 -1")
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestListModifyCollKeepsForm guards the three list mutators against the
// materialize-and-demote trap. LINSERT, LREM and LTRIM all routed through
// getList, which clones every element of a coll list onto the heap, then wrote the
// result back with db.Set(listEncode(...)), one blob holding the whole list, which
// also demoted the key from coll form to blob. On a list larger than the cap that
// is an OOM, and the demote means the next reader pays the materialize again. The
// streamed paths edit the position-keyed sub-tree in place and leave it coll.
func TestListModifyCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	elems := make([]string, 600)
	for i := range elems {
		elems[i] = fmt.Sprintf("e:%03d", i)
	}
	buildCollList(t, r, c, eng, "ins", elems)
	if got := sendLine(t, r, c, "LINSERT ins BEFORE e:300 NEW"); got != ":601" {
		t.Fatalf("LINSERT = %q want :601", got)
	}
	if !listIsColl(t, eng, "ins") {
		t.Fatal("LINSERT demoted the list out of coll form")
	}

	buildCollList(t, r, c, eng, "rem", elems)
	if got := sendLine(t, r, c, "LREM rem 0 e:300"); got != ":1" {
		t.Fatalf("LREM = %q want :1", got)
	}
	if !listIsColl(t, eng, "rem") {
		t.Fatal("LREM demoted the list out of coll form")
	}

	buildCollList(t, r, c, eng, "trim", elems)
	if got := sendLine(t, r, c, "LTRIM trim 100 500"); got != "+OK" {
		t.Fatalf("LTRIM = %q want +OK", got)
	}
	if !listIsColl(t, eng, "trim") {
		t.Fatal("LTRIM demoted the list out of coll form")
	}
}

// TestListModifyCollIsBounded witnesses that the cheap cases of each mutator stay
// flat in allocations on a large coll list rather than scaling with the list
// length the way a materialize would: an end trim deletes a couple of rows, a
// near-head insert shifts a couple of rows, and a tail remove drops one row, none
// of which clones the list.
func TestListModifyCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 2000
	build := func(key string) {
		for i := range n {
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("RPUSH"), []byte(key), []byte(fmt.Sprintf("e:%05d", i))})
		}
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte(key)})
		if got := string(conn.OutBytes()); got != "$9\r\nquicklist\r\n" {
			t.Fatalf("%s not coll: OBJECT ENCODING = %q", key, got)
		}
	}

	// LTRIM dropping one element off the tail deletes a single row.
	build("t")
	trim := [][]byte{[]byte("LTRIM"), []byte("t"), []byte("0"), []byte("-2")}
	if allocs := testing.AllocsPerRun(10, func() {
		conn.ResetOut()
		d.Handle(conn, trim)
	}); allocs > 200 {
		t.Fatalf("LTRIM end-trim over a %d-element coll list allocated %.0f per run", n, allocs)
	}

	// LINSERT before the head element shifts no prefix; the LPOP undo keeps the
	// list size and the pivot position stable across runs, and both are O(1).
	build("i")
	ins := [][]byte{[]byte("LINSERT"), []byte("i"), []byte("BEFORE"), []byte("e:00000"), []byte("x")}
	pop := [][]byte{[]byte("LPOP"), []byte("i")}
	if allocs := testing.AllocsPerRun(10, func() {
		conn.ResetOut()
		d.Handle(conn, ins)
		conn.ResetOut()
		d.Handle(conn, pop)
	}); allocs > 300 {
		t.Fatalf("LINSERT at head over a %d-element coll list allocated %.0f per run", n, allocs)
	}

	// LREM of the single match at the tail drops one row and shifts nothing.
	build("r")
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("RPUSH"), []byte("r"), []byte("tailonly")})
	rem := [][]byte{[]byte("LREM"), []byte("r"), []byte("-1"), []byte("tailonly")}
	put := [][]byte{[]byte("RPUSH"), []byte("r"), []byte("tailonly")}
	if allocs := testing.AllocsPerRun(10, func() {
		conn.ResetOut()
		d.Handle(conn, rem)
		conn.ResetOut()
		d.Handle(conn, put) // restore the tail match for the next run
	}); allocs > 300 {
		t.Fatalf("LREM tail-match over a %d-element coll list allocated %.0f per run", n, allocs)
	}
}

// TestLInsertCollMatchesNaive checks the streamed coll-form LINSERT against the
// model for both shift directions, both BEFORE and AFTER, and a missing pivot.
func TestLInsertCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	model := make([]string, 600)
	for i := range model {
		model[i] = fmt.Sprintf("e:%03d", i)
	}
	buildCollList(t, r, c, eng, "l", model)

	insert := func(pos int, val string) {
		out := make([]string, 0, len(model)+1)
		out = append(out, model[:pos]...)
		out = append(out, val)
		out = append(out, model[pos:]...)
		model = out
	}

	// BEFORE a pivot near the head shifts the short prefix.
	if got := sendLine(t, r, c, "LINSERT l BEFORE e:005 H"); got != fmt.Sprintf(":%d", len(model)+1) {
		t.Fatalf("LINSERT BEFORE near head = %q", got)
	}
	insert(5, "H")
	// AFTER a pivot near the tail shifts the short suffix.
	if got := sendLine(t, r, c, "LINSERT l AFTER e:595 T"); got != fmt.Sprintf(":%d", len(model)+1) {
		t.Fatalf("LINSERT AFTER near tail = %q", got)
	}
	// e:595 now sits one past its original index because of the head insert.
	insert(597, "T")
	// A missing pivot returns -1 and changes nothing.
	if got := sendLine(t, r, c, "LINSERT l BEFORE nope Z"); got != ":-1" {
		t.Fatalf("LINSERT missing pivot = %q want :-1", got)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("LINSERT demoted the list")
	}
	if got := collListContents(t, r, c, "l"); !eqStrings(got, model) {
		t.Fatalf("after LINSERT list mismatch:\n got len %d\nwant len %d", len(got), len(model))
	}
}

// TestLRemCollMatchesNaive checks the streamed coll-form LREM against the model
// for forward, backward and remove-all counts, and for emptying the list.
func TestLRemCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	// 600 elements with "x" planted every 50 positions: indices 0,50,...,550 (12).
	base := make([]string, 600)
	for i := range base {
		if i%50 == 0 {
			base[i] = "x"
		} else {
			base[i] = fmt.Sprintf("e:%03d", i)
		}
	}
	naiveRem := func(elems []string, count int, target string) []string {
		out := make([]string, 0, len(elems))
		if count >= 0 {
			removed := 0
			for _, e := range elems {
				if e == target && (count == 0 || removed < count) {
					removed++
					continue
				}
				out = append(out, e)
			}
			return out
		}
		// Negative count: drop the last |count| matches.
		limit := -count
		keep := make([]bool, len(elems))
		for i := range keep {
			keep[i] = true
		}
		dropped := 0
		for i := len(elems) - 1; i >= 0 && dropped < limit; i-- {
			if elems[i] == target {
				keep[i] = false
				dropped++
			}
		}
		for i, e := range elems {
			if keep[i] {
				out = append(out, e)
			}
		}
		return out
	}

	cases := []struct {
		count int
	}{{2}, {-2}, {0}, {100}}
	for _, tc := range cases {
		buildCollList(t, r, c, eng, "l", base)
		want := naiveRem(base, tc.count, "x")
		nrem := len(base) - len(want)
		if got := sendLine(t, r, c, fmt.Sprintf("LREM l %d x", tc.count)); got != fmt.Sprintf(":%d", nrem) {
			t.Fatalf("LREM l %d x = %q want :%d", tc.count, got, nrem)
		}
		if !listIsColl(t, eng, "l") {
			t.Fatalf("LREM count %d demoted the list", tc.count)
		}
		if got := collListContents(t, r, c, "l"); !eqStrings(got, want) {
			t.Fatalf("LREM count %d contents mismatch: got %d want %d elems", tc.count, len(got), len(want))
		}
		sendLine(t, r, c, "DEL l")
	}

	// Removing every element empties the list, which deletes the key. The element
	// is wide enough that 200 copies spill the blob past the inline cap into coll
	// form, since a short repeated value would stay a small blob below it.
	xval := "xxxxxxxxxxxxxxxx"
	allx := make([]string, 200)
	for i := range allx {
		allx[i] = xval
	}
	buildCollList(t, r, c, eng, "l", allx)
	if got := sendLine(t, r, c, "LREM l 0 "+xval); got != ":200" {
		t.Fatalf("LREM all = %q want :200", got)
	}
	if got := sendLine(t, r, c, "EXISTS l"); got != ":0" {
		t.Fatalf("emptied list still exists: EXISTS = %q", got)
	}
}

// TestLTrimCollMatchesNaive checks the streamed coll-form LTRIM against the model
// for an interior window, negative indices, a no-op full range and an empty range
// that deletes the key.
func TestLTrimCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	base := make([]string, 600)
	for i := range base {
		base[i] = fmt.Sprintf("e:%03d", i)
	}
	naiveSlice := func(elems []string, start, stop int) []string {
		n := len(elems)
		if start < 0 {
			start += n
		}
		if stop < 0 {
			stop += n
		}
		if start < 0 {
			start = 0
		}
		if start >= n || start > stop {
			return []string{}
		}
		if stop >= n {
			stop = n - 1
		}
		return append([]string(nil), elems[start:stop+1]...)
	}

	cases := []struct{ start, stop int }{{100, 500}, {-200, -1}, {0, -1}}
	for _, tc := range cases {
		buildCollList(t, r, c, eng, "l", base)
		want := naiveSlice(base, tc.start, tc.stop)
		if got := sendLine(t, r, c, fmt.Sprintf("LTRIM l %d %d", tc.start, tc.stop)); got != "+OK" {
			t.Fatalf("LTRIM l %d %d = %q", tc.start, tc.stop, got)
		}
		if !listIsColl(t, eng, "l") {
			t.Fatalf("LTRIM %d %d demoted the list", tc.start, tc.stop)
		}
		if got := collListContents(t, r, c, "l"); !eqStrings(got, want) {
			t.Fatalf("LTRIM %d %d contents mismatch: got %d want %d", tc.start, tc.stop, len(got), len(want))
		}
		sendLine(t, r, c, "DEL l")
	}

	// An empty range deletes the key.
	buildCollList(t, r, c, eng, "l", base)
	if got := sendLine(t, r, c, "LTRIM l 500 100"); got != "+OK" {
		t.Fatalf("LTRIM empty range = %q", got)
	}
	if got := sendLine(t, r, c, "EXISTS l"); got != ":0" {
		t.Fatalf("emptied list still exists: EXISTS = %q", got)
	}
}
