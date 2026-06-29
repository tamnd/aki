package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRangeByLexCollIsBounded guards ZRANGEBYLEX, ZREVRANGEBYLEX, and ZLEXCOUNT
// against the materialize trap on a coll-form sorted set. All three went through
// getZSet, which clones every member onto the heap even to return a narrow lex band:
// O(n) allocation for a query that touches a handful of rows, and an OOM under a tight
// cap on a multi-million-member set. The bounded path seeks the member index straight
// to the bound and walks only the matching window.
//
// The witness is allocation count over a narrow band: a small window costs a small
// constant no matter how big the set is. Every member shares one score, as the lex
// commands assume, so member byte order is the rank order. Members are padded so a
// whole-set clone would move about a megabyte.
func TestZRangeByLexCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		return []byte(fmt.Sprintf("m%05d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte("0"), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	lo := append([]byte("["), member(2000)...)
	hi := append([]byte("["), member(2004)...)
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANGEBYLEX"), []byte("z"), lo, hi})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZREVRANGEBYLEX"), []byte("z"), hi, lo})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZLEXCOUNT"), []byte("z"), lo, hi})
	})
	if allocs > 300 {
		t.Fatalf("ZRANGEBYLEX+ZREVRANGEBYLEX+ZLEXCOUNT over a 5-member band of a %d-member set "+
			"allocated %.0f objects per run; a narrow band should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRangeByLexCollMatchesBlob checks the coll-form member walk returns exactly
// what the materialized path would: forward ascending, reverse descending, the
// inclusive/exclusive bracket rules, the - and + infinities, and LIMIT slicing.
func TestZRangeByLexCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z 0 m:%06d", i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Inclusive band ascending.
	got := readArray(t, r, c, "ZRANGEBYLEX z [m:000100 [m:000109")
	if len(got) != 10 || got[0] != "m:000100" || got[9] != "m:000109" {
		t.Fatalf("inclusive band = %v", got)
	}

	// Exclusive low bound drops the first member.
	ex := readArray(t, r, c, "ZRANGEBYLEX z (m:000100 [m:000109")
	if len(ex) != 9 || ex[0] != "m:000101" {
		t.Fatalf("exclusive-low band = %v", ex)
	}

	// LIMIT slices after the bounds apply.
	lim := readArray(t, r, c, "ZRANGEBYLEX z [m:000100 [m:000200 LIMIT 5 3")
	if len(lim) != 3 || lim[0] != "m:000105" || lim[2] != "m:000107" {
		t.Fatalf("LIMIT band = %v", lim)
	}

	// Full range with the infinities.
	if cnt := sendLine(t, r, c, "ZLEXCOUNT z - +"); cnt != fmt.Sprintf(":%d", n) {
		t.Fatalf("ZLEXCOUNT - + = %q want :%d", cnt, n)
	}
	if cnt := sendLine(t, r, c, "ZLEXCOUNT z (m:000100 (m:000110"); cnt != ":9" {
		t.Fatalf("ZLEXCOUNT exclusive band = %q want :9", cnt)
	}

	// Reverse band descending. ZREVRANGEBYLEX takes max then min.
	rev := readArray(t, r, c, "ZREVRANGEBYLEX z [m:000109 [m:000100")
	if len(rev) != 10 || rev[0] != "m:000109" || rev[9] != "m:000100" {
		t.Fatalf("reverse band = %v", rev)
	}

	// Reverse exclusive high drops the top member.
	revEx := readArray(t, r, c, "ZREVRANGEBYLEX z (m:000109 [m:000100")
	if len(revEx) != 9 || revEx[0] != "m:000108" {
		t.Fatalf("reverse exclusive-high band = %v", revEx)
	}

	// Reverse full range, highest first.
	all := readArray(t, r, c, "ZREVRANGEBYLEX z + -")
	if len(all) != n || all[0] != "m:000999" || all[n-1] != "m:000000" {
		t.Fatalf("reverse full range: len=%d first=%q last=%q", len(all), all[0], all[len(all)-1])
	}

	// Reverse LIMIT.
	revLim := readArray(t, r, c, "ZREVRANGEBYLEX z [m:000200 [m:000100 LIMIT 5 3")
	if len(revLim) != 3 || revLim[0] != "m:000195" || revLim[2] != "m:000193" {
		t.Fatalf("reverse LIMIT band = %v", revLim)
	}
}
