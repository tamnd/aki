package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRevRangeByScoreCollIsBounded guards ZREVRANGEBYSCORE against the materialize
// trap on a coll-form sorted set. The forward path got bounded first; the reverse
// direction kept going through getZSet, which clones every member onto the heap and
// reverses the lot, even to hand back a narrow score band: O(n) allocation for a
// query that touches a handful of rows, and an OOM kill under a tight cap on a
// multi-million-member set. The bounded path seeks the score index to just past the
// high bound and walks backward over only the matching window.
//
// The witness is allocation count over a narrow band: a five-element reverse window
// costs a small constant no matter how big the set is. We build a set well past the
// skiplist threshold with padded members so a whole-set clone would move about a
// megabyte, then assert the reverse band read stays bounded.
func TestZRevRangeByScoreCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		return []byte(fmt.Sprintf("%08d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte(strconv.Itoa(i)), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	// A five-score band, queried high-then-low as ZREVRANGEBYSCORE takes its args.
	hi, lo := []byte("2004"), []byte("2000")
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZREVRANGEBYSCORE"), []byte("z"), hi, lo})
	})
	// Five members. A whole-set clone-and-reverse would be on the order of n; bound
	// well below n so the materialize path cannot return.
	if allocs > 200 {
		t.Fatalf("ZREVRANGEBYSCORE over a 5-score band of a %d-member set allocated %.0f "+
			"objects per run; a narrow reverse band should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRevRangeByScoreCollMatchesBlob checks the coll-form backward score walk returns
// exactly what the materialized path would: the same members in descending score
// order, with the LIMIT and exclusive-bound rules applied the same way. It drives a
// large (coll) sorted set through the queries and compares against hand-computed
// expectations that mirror Redis semantics.
func TestZRevRangeByScoreCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Inclusive band, descending. ZREVRANGEBYSCORE takes max then min.
	got := readArray(t, r, c, "ZREVRANGEBYSCORE z 109 100 WITHSCORES")
	if len(got) != 20 {
		t.Fatalf("inclusive band returned %d elements want 20: %v", len(got), got)
	}
	for i := 0; i < 10; i++ {
		wantM := fmt.Sprintf("m:%06d", 109-i)
		wantS := strconv.Itoa(109 - i)
		if got[i*2] != wantM || got[i*2+1] != wantS {
			t.Fatalf("band element %d = (%q,%q) want (%q,%q)", i, got[i*2], got[i*2+1], wantM, wantS)
		}
	}

	// Exclusive high bound drops the 109 score, so the walk starts at 108.
	ex := readArray(t, r, c, "ZREVRANGEBYSCORE z (109 100")
	if len(ex) != 9 || ex[0] != "m:000108" {
		t.Fatalf("exclusive-high band = %v", ex)
	}

	// LIMIT offset/count slices the band after the bounds apply, descending.
	lim := readArray(t, r, c, "ZREVRANGEBYSCORE z 200 100 LIMIT 5 3")
	if len(lim) != 3 || lim[0] != "m:000195" || lim[2] != "m:000193" {
		t.Fatalf("LIMIT band = %v", lim)
	}

	// Open bounds cover the whole set, highest first.
	all := readArray(t, r, c, "ZREVRANGEBYSCORE z +inf -inf")
	if len(all) != n || all[0] != "m:000999" || all[n-1] != "m:000000" {
		t.Fatalf("open reverse range: len=%d first=%q last=%q", len(all), all[0], all[len(all)-1])
	}
}
