package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRankCollIsBounded guards ZRANK and ZREVRANK against the materialize trap on a
// coll-form sorted set. Both went through getZSet, which clones every member onto the
// heap just to find one member's position: O(n) allocation for a point query, and an
// OOM under a tight cap on a multi-million-member set. The bounded path point-looks up
// the member's score and counts the score rows before it without cloning.
//
// The witness is allocation count for ranking a member near the front: it stays a small
// constant. We rank near the front so the count walk is short; the point of the test is
// the absence of the whole-set clone, not the walk length.
func TestZRankCollIsBounded(t *testing.T) {
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

	m3 := member(3)
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANK"), []byte("z"), m3})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZREVRANK"), []byte("z"), m3})
	})
	if allocs > 400 {
		t.Fatalf("ZRANK+ZREVRANK of a member near the front of a %d-member set allocated %.0f "+
			"objects per run; a bounded rank should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRankCollMatchesBlob checks the coll-form rank lookup returns exactly what the
// materialized path would: the ascending rank, the descending rank, the WITHSCORE
// pairing, and a null for an absent member.
func TestZRankCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// Ascending rank equals the score for this seeding.
	if got := sendLine(t, r, c, "ZRANK z m:000000"); got != ":0" {
		t.Fatalf("ZRANK m:000000 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZRANK z m:000500"); got != ":500" {
		t.Fatalf("ZRANK m:000500 = %q want :500", got)
	}
	if got := sendLine(t, r, c, "ZRANK z m:000999"); got != ":999" {
		t.Fatalf("ZRANK m:000999 = %q want :999", got)
	}

	// Descending rank is card-1 minus the ascending rank.
	if got := sendLine(t, r, c, "ZREVRANK z m:000999"); got != ":0" {
		t.Fatalf("ZREVRANK m:000999 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZREVRANK z m:000500"); got != ":499" {
		t.Fatalf("ZREVRANK m:000500 = %q want :499", got)
	}

	// WITHSCORE returns a two-element reply: the rank as an integer, the score as a
	// bulk double. readElem keeps the ":" prefix on the integer element.
	got := readArray(t, r, c, "ZRANK z m:000100 WITHSCORE")
	if len(got) != 2 || got[0] != ":100" || got[1] != "100" {
		t.Fatalf("ZRANK m:000100 WITHSCORE = %v want [:100 100]", got)
	}

	// An absent member is a null reply.
	if got := sendLine(t, r, c, "ZRANK z nope"); got != "$-1" && got != "_" {
		t.Fatalf("ZRANK absent = %q want null", got)
	}
}
