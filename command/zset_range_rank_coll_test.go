package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRangeRankCollIsBounded guards a whole-set ZRANGE / ZREVRANGE by-rank dump
// against the full-reply materialize trap on a coll-form sorted set. The by-rank
// coll path used to clone the requested window into a []zmember the caller then
// wrote; for the canonical ZRANGE key 0 -1 the window is the whole set, so that is
// O(n) transient heap on top of the O(n) reply bytes, an OOM under a tight cap on a
// set larger than RAM. The streaming path writes each member straight off the
// score-index cursor, so retained memory is the cursor pages plus the flush buffer,
// never a whole-set clone.
//
// The witness is allocation count with the output buffer reused across runs: the
// streamed dump allocates a small constant, far below the per-run clone an n-member
// []zmember would cost.
func TestZRangeRankCollIsBounded(t *testing.T) {
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

	// The whole-set dump in each direction, with and without scores: every shape is
	// the FULL reply, and each must stay a small constant in allocation.
	for _, cmd := range [][][]byte{
		{[]byte("ZRANGE"), []byte("z"), []byte("0"), []byte("-1")},
		{[]byte("ZRANGE"), []byte("z"), []byte("0"), []byte("-1"), []byte("WITHSCORES")},
		{[]byte("ZREVRANGE"), []byte("z"), []byte("0"), []byte("-1")},
		{[]byte("ZREVRANGE"), []byte("z"), []byte("0"), []byte("-1"), []byte("WITHSCORES")},
	} {
		args := cmd
		allocs := testing.AllocsPerRun(20, func() {
			conn.ResetOut()
			d.Handle(conn, args)
		})
		// A materialize clones all n members (each a padded ~248-byte key) every run.
		// The streamed dump touches only the cursor and reader, a small constant.
		if allocs > 60 {
			t.Fatalf("%s 0 -1 over a %d-member coll-form zset allocated %.0f objects per run; "+
				"a streamed dump should be a small constant, not O(n)", args[0], n, allocs)
		}
	}
}

// TestZRangeRankCollMatchesBlob checks the streamed coll-form by-rank dump carries
// exactly what the materialized path would: ZRANGE ascending by score, ZREVRANGE
// descending, the WITHSCORES interleave, partial windows off each end, and negative
// indices. It drives a large (coll) sorted set and compares against the known order.
func TestZRangeRankCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Whole-set ascending dump: every member in score order.
	asc := readArray(t, r, c, "ZRANGE z 0 -1")
	if len(asc) != n {
		t.Fatalf("ZRANGE z 0 -1 returned %d want %d", len(asc), n)
	}
	for i := range asc {
		if asc[i] != fmt.Sprintf("m:%06d", i) {
			t.Fatalf("ZRANGE z 0 -1 element %d = %q want m:%06d", i, asc[i], i)
		}
	}

	// Whole-set descending dump: reverse score order.
	desc := readArray(t, r, c, "ZREVRANGE z 0 -1")
	if len(desc) != n {
		t.Fatalf("ZREVRANGE z 0 -1 returned %d want %d", len(desc), n)
	}
	for i := range desc {
		if desc[i] != fmt.Sprintf("m:%06d", n-1-i) {
			t.Fatalf("ZREVRANGE z 0 -1 element %d = %q want m:%06d", i, desc[i], n-1-i)
		}
	}

	// WITHSCORES interleaves member and score in order.
	ws := readArray(t, r, c, "ZRANGE z 0 2 WITHSCORES")
	if len(ws) != 6 || ws[0] != "m:000000" || ws[1] != "0" || ws[4] != "m:000002" || ws[5] != "2" {
		t.Fatalf("ZRANGE z 0 2 WITHSCORES = %v", ws)
	}

	// A window off the front and a window off the back, both partial.
	front := readArray(t, r, c, "ZRANGE z 0 4")
	if len(front) != 5 || front[0] != "m:000000" || front[4] != "m:000004" {
		t.Fatalf("ZRANGE z 0 4 = %v", front)
	}
	back := readArray(t, r, c, "ZRANGE z -3 -1")
	if len(back) != 3 || back[0] != fmt.Sprintf("m:%06d", n-3) || back[2] != fmt.Sprintf("m:%06d", n-1) {
		t.Fatalf("ZRANGE z -3 -1 = %v", back)
	}

	// A reverse window off the front (highest scores).
	revfront := readArray(t, r, c, "ZREVRANGE z 0 2")
	if len(revfront) != 3 || revfront[0] != fmt.Sprintf("m:%06d", n-1) || revfront[2] != fmt.Sprintf("m:%06d", n-3) {
		t.Fatalf("ZREVRANGE z 0 2 = %v", revfront)
	}

	// An out-of-range window is an empty array, not an error.
	if got := readArray(t, r, c, "ZRANGE z 5000 6000"); len(got) != 0 {
		t.Fatalf("ZRANGE z 5000 6000 = %v want empty", got)
	}
}

// TestZRangeRankCollStreamsLargeReply drives a dump several times the 64 KiB flush
// threshold over a real connection, so StreamFlush actually spills mid-command, and
// checks the client reassembles the exact member sequence across the chunk
// boundaries.
func TestZRangeRankCollStreamsLargeReply(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'y'
	}
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d:%s", i, i, string(pad)))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	got := readArray(t, r, c, "ZRANGE z 0 -1")
	if len(got) != n {
		t.Fatalf("ZRANGE z 0 -1 over a quarter-MB reply returned %d want %d", len(got), n)
	}
	for i := range got {
		want := fmt.Sprintf("m:%06d:%s", i, string(pad))
		if got[i] != want {
			t.Fatalf("element %d mismatched after mid-reply flush", i)
		}
	}
}
