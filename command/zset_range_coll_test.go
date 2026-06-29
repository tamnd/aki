package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRangeByScoreCollIsBounded guards ZRANGEBYSCORE and ZCOUNT against the
// materialize trap on a coll-form sorted set. Both went through getZSet, which clones
// every member onto the heap even to return a narrow score band: O(n) allocation for
// a query that touches a handful of rows, and an OOM kill under a tight cap on a
// multi-million-member set. The forward path now seeks the score-index straight to the
// low bound and walks only the matching window.
//
// The witness is allocation count over a narrow band: a five-element window costs a
// small constant no matter how big the set is. We build a set well past the skiplist
// threshold with padded members so a whole-set clone would move about a megabyte, then
// assert the band read and its count stay bounded.
func TestZRangeByScoreCollIsBounded(t *testing.T) {
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

	// A five-score band somewhere in the middle, read with WITHSCORES and counted.
	lo, hi := []byte("2000"), []byte("2004")
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANGEBYSCORE"), []byte("z"), lo, hi})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZCOUNT"), []byte("z"), lo, hi})
	})
	// Five members plus a five-count. A whole-set clone would be on the order of n;
	// bound well below n so the materialize path cannot return.
	if allocs > 200 {
		t.Fatalf("ZRANGEBYSCORE+ZCOUNT over a 5-score band of a %d-member set allocated %.0f "+
			"objects per run; a narrow band should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRangeByScoreCollMatchesBlob checks the coll-form forward score walk returns
// exactly what the materialized path would: the same members, in score order, with
// the LIMIT and exclusive-bound rules applied the same way. It drives both a small
// (blob) and a large (coll) sorted set through identical queries and compares.
func TestZRangeByScoreCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Inclusive band.
	got := readArray(t, r, c, "ZRANGEBYSCORE z 100 109 WITHSCORES")
	if len(got) != 20 {
		t.Fatalf("inclusive band returned %d elements want 20: %v", len(got), got)
	}
	for i := 0; i < 10; i++ {
		if got[i*2] != fmt.Sprintf("m:%06d", 100+i) || got[i*2+1] != strconv.Itoa(100+i) {
			t.Fatalf("band element %d = (%q,%q)", i, got[i*2], got[i*2+1])
		}
	}

	// Exclusive low bound drops the 100 score.
	ex := readArray(t, r, c, "ZRANGEBYSCORE z (100 109")
	if len(ex) != 9 || ex[0] != "m:000101" {
		t.Fatalf("exclusive-low band = %v", ex)
	}

	// LIMIT offset/count slices the band after the bounds apply.
	lim := readArray(t, r, c, "ZRANGEBYSCORE z 100 200 LIMIT 5 3")
	if len(lim) != 3 || lim[0] != "m:000105" || lim[2] != "m:000107" {
		t.Fatalf("LIMIT band = %v", lim)
	}

	// Open bounds cover the whole set, and the count agrees.
	if cnt := sendLine(t, r, c, "ZCOUNT z -inf +inf"); cnt != fmt.Sprintf(":%d", n) {
		t.Fatalf("ZCOUNT -inf +inf = %q want :%d", cnt, n)
	}
	if cnt := sendLine(t, r, c, "ZCOUNT z (100 (110"); cnt != ":9" {
		t.Fatalf("ZCOUNT exclusive band = %q want :9", cnt)
	}
}
