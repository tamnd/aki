package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRangeByRankCollIsBounded guards ZRANGE and ZREVRANGE by rank against the
// materialize trap on a coll-form sorted set. Both went through getZSet, which clones
// every member onto the heap even to return a top-N or bottom-N slice: O(n) allocation
// for a query that returns a handful of rows, and an OOM under a tight cap on a
// multi-million-member set. The bounded path seeks the nearer end of the score index
// and skips with the cursor, so a slice off either end stays O(window).
//
// The witness is allocation count for a small slice taken off each end: it stays a
// small constant no matter how big the set is. Members are padded so a whole-set clone
// would move about a megabyte.
func TestZRangeByRankCollIsBounded(t *testing.T) {
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

	// A five-element slice off the front, off the back, and the reverse top five.
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANGE"), []byte("z"), []byte("0"), []byte("4")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANGE"), []byte("z"), []byte("-5"), []byte("-1")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZREVRANGE"), []byte("z"), []byte("0"), []byte("4")})
	})
	if allocs > 300 {
		t.Fatalf("ZRANGE/ZREVRANGE of 5-element slices off a %d-member set allocated %.0f objects "+
			"per run; a bounded slice should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRangeByRankCollMatchesBlob checks the coll-form by-rank walk returns exactly
// what the materialized path would: forward slices off each end, the middle, negative
// indices, WITHSCORES pairing, the reverse direction, and clamping past the bounds.
func TestZRangeByRankCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Front slice ascending.
	got := readArray(t, r, c, "ZRANGE z 0 4")
	want := []string{"m:000000", "m:000001", "m:000002", "m:000003", "m:000004"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZRANGE 0 4 elem %d = %q want %q", i, got[i], want[i])
		}
	}

	// Back slice via negative indices.
	got = readArray(t, r, c, "ZRANGE z -3 -1")
	want = []string{"m:000997", "m:000998", "m:000999"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZRANGE -3 -1 elem %d = %q want %q", i, got[i], want[i])
		}
	}

	// Middle slice, deep enough to force a skip from the front.
	got = readArray(t, r, c, "ZRANGE z 500 503")
	want = []string{"m:000500", "m:000501", "m:000502", "m:000503"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZRANGE 500 503 elem %d = %q want %q", i, got[i], want[i])
		}
	}

	// WITHSCORES pairs member then score.
	got = readArray(t, r, c, "ZRANGE z 0 1 WITHSCORES")
	if len(got) != 4 || got[0] != "m:000000" || got[1] != "0" || got[2] != "m:000001" || got[3] != "1" {
		t.Fatalf("ZRANGE 0 1 WITHSCORES = %v", got)
	}

	// Reverse: rank 0 is the highest score, descending.
	got = readArray(t, r, c, "ZREVRANGE z 0 4")
	want = []string{"m:000999", "m:000998", "m:000997", "m:000996", "m:000995"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZREVRANGE 0 4 elem %d = %q want %q", i, got[i], want[i])
		}
	}

	// Reverse negative indices reach the low end.
	got = readArray(t, r, c, "ZREVRANGE z -2 -1")
	want = []string{"m:000001", "m:000000"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZREVRANGE -2 -1 elem %d = %q want %q", i, got[i], want[i])
		}
	}

	// Stop past the end clamps; start past the end is empty.
	got = readArray(t, r, c, "ZRANGE z 998 5000")
	if len(got) != 2 || got[0] != "m:000998" || got[1] != "m:000999" {
		t.Fatalf("ZRANGE 998 5000 = %v", got)
	}
	if empty := readArray(t, r, c, "ZRANGE z 5000 6000"); len(empty) != 0 {
		t.Fatalf("ZRANGE 5000 6000 = %v want empty", empty)
	}
}
