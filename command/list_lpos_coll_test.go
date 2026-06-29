package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestLPosCollIsBounded guards LPOS against the materialize trap on a coll-form
// list. LPOS routed through getList, which clones every element of a coll list
// onto the heap before the first comparison, so a position lookup over a
// million-element list dragged the whole list through memory, an OOM under a tight
// cap for a query whose answer is one integer. The streamed path walks the index
// window through an arena cursor and retains only the matched positions.
//
// The witness is allocation count for a no-match LPOS over a large coll list: the
// streamed path compares every element in place and keeps nothing, so it stays a
// small constant while a materialize would clone all n elements.
func TestLPosCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 2000
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("RPUSH"), []byte("l"), []byte(fmt.Sprintf("e:%05d", i))})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("l")})
	if got := string(conn.OutBytes()); got != "$9\r\nquicklist\r\n" {
		t.Fatalf("list not in coll form: OBJECT ENCODING = %q", got)
	}

	// A miss compares every element but retains nothing.
	miss := [][]byte{[]byte("LPOS"), []byte("l"), []byte("absent"), []byte("COUNT"), []byte("0")}
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, miss)
	})
	if allocs > 200 {
		t.Fatalf("LPOS over a %d-element coll list allocated %.0f objects per run; "+
			"the streamed path should compare in place, not clone n", n, allocs)
	}

	// A first-match lookup near the head stops after a handful of comparisons.
	hit := [][]byte{[]byte("LPOS"), []byte("l"), []byte("e:00003")}
	allocs = testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, hit)
	})
	if allocs > 200 {
		t.Fatalf("LPOS first-match near the head allocated %.0f objects per run", allocs)
	}
}

// TestLPosCollMatchesNaive checks the streamed coll-form LPOS returns exactly what
// the materialized scan would: the first match, a chosen rank, a backward rank,
// the COUNT cap, COUNT 0 for all matches, the MAXLEN compare cap, and a miss.
func TestLPosCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	// Build a coll list of 600 elements where "x" sits at positions 10, 200, and
	// 590, well past the 128-entry quicklist threshold so the list is coll.
	want := map[int]bool{10: true, 200: true, 590: true}
	for i := range 600 {
		v := fmt.Sprintf("e:%03d", i)
		if want[i] {
			v = "x"
		}
		if got := sendLine(t, r, c, "RPUSH l "+v); got != fmt.Sprintf(":%d", i+1) {
			t.Fatalf("RPUSH %d = %q", i, got)
		}
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("600-element list should be stored coll")
	}

	// No options: the first match head to tail.
	if got := sendLine(t, r, c, "LPOS l x"); got != ":10" {
		t.Fatalf("LPOS l x = %q want :10", got)
	}
	// RANK 2: the second match head to tail.
	if got := sendLine(t, r, c, "LPOS l x RANK 2"); got != ":200" {
		t.Fatalf("LPOS l x RANK 2 = %q want :200", got)
	}
	// RANK -1: the first match scanning tail to head.
	if got := sendLine(t, r, c, "LPOS l x RANK -1"); got != ":590" {
		t.Fatalf("LPOS l x RANK -1 = %q want :590", got)
	}
	// RANK -2: the second match scanning tail to head.
	if got := sendLine(t, r, c, "LPOS l x RANK -2"); got != ":200" {
		t.Fatalf("LPOS l x RANK -2 = %q want :200", got)
	}
	// COUNT 2: the first two matches in an array.
	if got := readArray(t, r, c, "LPOS l x COUNT 2"); len(got) != 2 || got[0] != ":10" || got[1] != ":200" {
		t.Fatalf("LPOS l x COUNT 2 = %v want [:10 :200]", got)
	}
	// COUNT 0: every match.
	if got := readArray(t, r, c, "LPOS l x COUNT 0"); len(got) != 3 || got[0] != ":10" || got[2] != ":590" {
		t.Fatalf("LPOS l x COUNT 0 = %v want [:10 :200 :590]", got)
	}
	// COUNT 0 RANK -1: every match scanning from the tail, tail-first order.
	if got := readArray(t, r, c, "LPOS l x RANK -1 COUNT 0"); len(got) != 3 || got[0] != ":590" || got[2] != ":10" {
		t.Fatalf("LPOS l x RANK -1 COUNT 0 = %v want [:590 :200 :10]", got)
	}
	// MAXLEN 50: only the first 50 elements are compared, so position 10 is found
	// but 200 and 590 are past the cap.
	if got := readArray(t, r, c, "LPOS l x COUNT 0 MAXLEN 50"); len(got) != 1 || got[0] != ":10" {
		t.Fatalf("LPOS l x COUNT 0 MAXLEN 50 = %v want [:10]", got)
	}
	// A miss returns a null (no COUNT) and an empty array (with COUNT).
	if got := sendLine(t, r, c, "LPOS l absent"); got != "$-1" {
		t.Fatalf("LPOS l absent = %q want null", got)
	}
	if got := readArray(t, r, c, "LPOS l absent COUNT 0"); len(got) != 0 {
		t.Fatalf("LPOS l absent COUNT 0 = %v want empty", got)
	}
}
