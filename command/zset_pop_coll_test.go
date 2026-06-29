package command

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZPopCollIsBounded guards ZPOPMIN and ZPOPMAX against the materialize trap on
// a coll-form sorted set. Both went through getZSet, which clones every member onto
// the heap, then db.Set, which rewrote the whole set as a blob (demoting it): O(n)
// allocation and an O(n) write to pop a handful of members, and an OOM kill under a
// tight cap on a multi-million-member set. The bounded path seeks straight to the
// end it pops from and walks only the popped window.
//
// The witness is allocation count per pop of a fixed small count: it stays a small
// constant no matter how big the set is. We build a set well past the skiplist
// threshold with padded members so a whole-set clone would move about a megabyte,
// re-adding the popped members each run so the set size holds steady.
func TestZPopCollIsBounded(t *testing.T) {
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
	add := func(i int) {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte(strconv.Itoa(i)), member(i)})
	}
	for i := range n {
		add(i)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	// Pop three off each end, then put them back so the next run starts from the
	// same size. The re-add is outside the witness.
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZPOPMIN"), []byte("z"), []byte("3")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZPOPMAX"), []byte("z"), []byte("3")})
		add(0)
		add(1)
		add(2)
		add(n - 1)
		add(n - 2)
		add(n - 3)
	})
	if allocs > 400 {
		t.Fatalf("ZPOPMIN+ZPOPMAX of 3 each off a %d-member set allocated %.0f objects per run "+
			"(re-add included); a bounded pop should be a small constant, not O(n)", n, allocs)
	}
}

// TestZPopCollMatchesBlob checks the coll-form bounded pop returns exactly what the
// materialized path would: ZPOPMIN takes the lowest scores ascending, ZPOPMAX the
// highest descending, the reply pairs member then score, popping past the size
// empties and deletes the key, and a default (no count) pop returns a flat two
// element reply.
func TestZPopCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	reseed := func() {
		_ = sendLine(t, r, c, "DEL z")
		for i := range n {
			_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
		}
		if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
			t.Fatalf("zset encoding = %q want skiplist", enc)
		}
	}

	reseed()
	// ZPOPMIN count: lowest three scores, ascending, as (member, score) pairs.
	got := readArray(t, r, c, "ZPOPMIN z 3")
	want := []string{"m:000000", "0", "m:000001", "1", "m:000002", "2"}
	if len(got) != len(want) {
		t.Fatalf("ZPOPMIN z 3 = %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZPOPMIN z 3 elem %d = %q want %q", i, got[i], want[i])
		}
	}
	// ZPOPMAX count: highest three scores, descending.
	got = readArray(t, r, c, "ZPOPMAX z 3")
	want = []string{"m:000999", "999", "m:000998", "998", "m:000997", "997"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZPOPMAX z 3 elem %d = %q want %q", i, got[i], want[i])
		}
	}
	// The set still reports skiplist and the card dropped by six.
	if card := sendLine(t, r, c, "ZCARD z"); card != fmt.Sprintf(":%d", n-6) {
		t.Fatalf("ZCARD after pops = %q want :%d", card, n-6)
	}

	// Default pop (no count) returns a flat two element reply: member then score.
	got = readArray(t, r, c, "ZPOPMIN z")
	if len(got) != 2 || got[0] != "m:000003" || got[1] != "3" {
		t.Fatalf("ZPOPMIN z (default) = %v", got)
	}

	// Popping past the size empties the set and deletes the key.
	reseed()
	got = readArray(t, r, c, fmt.Sprintf("ZPOPMAX z %d", n+10))
	if len(got) != 2*n {
		t.Fatalf("ZPOPMAX z %d returned %d elements want %d", n+10, len(got), 2*n)
	}
	if ex := sendLine(t, r, c, "EXISTS z"); ex != ":0" {
		t.Fatalf("EXISTS z after draining pop = %q want :0", ex)
	}
}
