package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZRandMemberCollIsBounded guards ZRANDMEMBER against the materialize trap on a
// coll-form sorted set. It used to route through getZSet, which clones every member
// and score onto the heap to pick a handful: O(n) allocation for a query that returns
// a few rows, and an OOM under a tight cap on a multi-million-member zset. The bounded
// path samples through a reservoir cursor walk over the arena-backed member index.
func TestZRandMemberCollIsBounded(t *testing.T) {
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		return []byte(fmt.Sprintf("m:%08d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte(fmt.Sprintf("%d", i)), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANDMEMBER"), []byte("z"), []byte("5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANDMEMBER"), []byte("z"), []byte("-5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANDMEMBER"), []byte("z"), []byte("5"), []byte("WITHSCORES")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZRANDMEMBER"), []byte("z")})
	})
	// A materialize would clone all n members, each a ~248-byte key plus its score,
	// well past 4000 objects. A bounded reservoir over an arena-backed walk touches
	// only the picks, a small constant driven by the count rather than the total.
	if allocs > 800 {
		t.Fatalf("ZRANDMEMBER on a %d-member coll-form zset allocated %.0f objects per run; "+
			"a bounded sample should be a small constant, not O(n)", n, allocs)
	}
}

// TestZRandMemberCollMatchesBlob checks the coll-form sample returns the shapes the
// materialized path would: a single member with no count, a distinct sample for a
// positive count, a repeats-allowed sample for a negative count, the WITHSCORES pair
// shape, the cap at cardinality, and the empty-key replies.
func TestZRandMemberCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	all := make(map[string]float64, n)
	for i := range n {
		m := fmt.Sprintf("m:%06d", i)
		all[m] = float64(i)
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d %s", i, m))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}

	// No count: a single member of the zset.
	one := bulk(t, r, c, "ZRANDMEMBER z")
	if _, ok := all[one]; !ok {
		t.Fatalf("ZRANDMEMBER z = %q, not a member of the zset", one)
	}

	// Positive count: distinct members, all of the zset.
	got := readArray(t, r, c, "ZRANDMEMBER z 10")
	if len(got) != 10 {
		t.Fatalf("ZRANDMEMBER z 10 returned %d members want 10", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if _, ok := all[m]; !ok {
			t.Fatalf("ZRANDMEMBER z 10 returned %q, not a member of the zset", m)
		}
		if seen[m] {
			t.Fatalf("ZRANDMEMBER z 10 returned a duplicate %q for a positive count", m)
		}
		seen[m] = true
	}

	// Count past the cardinality clamps to the whole zset, still distinct.
	got = readArray(t, r, c, "ZRANDMEMBER z 5000")
	if len(got) != n {
		t.Fatalf("ZRANDMEMBER z 5000 returned %d members want %d", len(got), n)
	}

	// Negative count: exactly the magnitude, repeats allowed and all valid.
	got = readArray(t, r, c, "ZRANDMEMBER z -20")
	if len(got) != 20 {
		t.Fatalf("ZRANDMEMBER z -20 returned %d members want 20", len(got))
	}
	for _, m := range got {
		if _, ok := all[m]; !ok {
			t.Fatalf("ZRANDMEMBER z -20 returned %q, not a member of the zset", m)
		}
	}

	// WITHSCORES: member/score pairs, each score matching the stored value.
	got = readArray(t, r, c, "ZRANDMEMBER z 10 WITHSCORES")
	if len(got) != 20 {
		t.Fatalf("ZRANDMEMBER z 10 WITHSCORES returned %d elements want 20", len(got))
	}
	for i := 0; i < len(got); i += 2 {
		m := got[i]
		want, ok := all[m]
		if !ok {
			t.Fatalf("ZRANDMEMBER WITHSCORES returned %q, not a member of the zset", m)
		}
		if got[i+1] != fmt.Sprintf("%d", int(want)) {
			t.Fatalf("ZRANDMEMBER WITHSCORES score for %q = %q want %d", m, got[i+1], int(want))
		}
	}

	// A missing key: null without a count, empty array with one.
	if reply := sendLine(t, r, c, "ZRANDMEMBER missing"); reply != "$-1" && reply != "_" {
		t.Fatalf("ZRANDMEMBER missing = %q want null", reply)
	}
	if got := readArray(t, r, c, "ZRANDMEMBER missing 5"); len(got) != 0 {
		t.Fatalf("ZRANDMEMBER missing 5 = %v want empty array", got)
	}
}
