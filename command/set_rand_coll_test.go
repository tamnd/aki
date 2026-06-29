package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSRandMemberCollIsBounded guards SRANDMEMBER against the materialize trap on a
// coll-form set. It used to route through getSet, which clones every member onto the
// heap to pick a handful: O(n) allocation for a query that returns a few rows, and
// an OOM under a tight cap on a multi-million-member set. The bounded path samples
// through a reservoir cursor walk over an arena-backed cursor.
//
// The witness is allocation count for a small count: it stays far below the
// whole-set clone a materialize would cost.
func TestSRandMemberCollIsBounded(t *testing.T) {
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
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("s"), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("s")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("set not in coll form: OBJECT ENCODING = %q", got)
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SRANDMEMBER"), []byte("s"), []byte("5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SRANDMEMBER"), []byte("s"), []byte("-5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SRANDMEMBER"), []byte("s")})
	})
	// A materialize would clone all n members, each a ~248-byte key, well past 4000
	// objects. A bounded reservoir over an arena-backed walk touches only the picks,
	// a small constant driven by the count rather than the member total.
	if allocs > 600 {
		t.Fatalf("SRANDMEMBER on a %d-member coll-form set allocated %.0f objects per run; "+
			"a bounded sample should be a small constant, not O(n)", n, allocs)
	}
}

// TestSRandMemberCollMatchesBlob checks the coll-form sample returns the shapes the
// materialized path would: a single member with no count, a distinct sample for a
// positive count, a repeats-allowed sample for a negative count, the cap at
// cardinality, and the empty-key replies.
func TestSRandMemberCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	all := make(map[string]bool, n)
	for i := range n {
		m := fmt.Sprintf("m:%06d", i)
		all[m] = true
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s %s", m))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "hashtable" {
		t.Fatalf("set encoding = %q want hashtable", enc)
	}

	// No count: a single member of the set.
	one := bulk(t, r, c, "SRANDMEMBER s")
	if !all[one] {
		t.Fatalf("SRANDMEMBER s = %q, not a member of the set", one)
	}

	// Positive count: distinct members, all of the set.
	got := readArray(t, r, c, "SRANDMEMBER s 10")
	if len(got) != 10 {
		t.Fatalf("SRANDMEMBER s 10 returned %d members want 10", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if !all[m] {
			t.Fatalf("SRANDMEMBER s 10 returned %q, not a member of the set", m)
		}
		if seen[m] {
			t.Fatalf("SRANDMEMBER s 10 returned a duplicate %q for a positive count", m)
		}
		seen[m] = true
	}

	// Count past the cardinality clamps to the whole set, still distinct.
	got = readArray(t, r, c, "SRANDMEMBER s 5000")
	if len(got) != n {
		t.Fatalf("SRANDMEMBER s 5000 returned %d members want %d", len(got), n)
	}

	// Negative count: exactly the magnitude, repeats allowed and all valid.
	got = readArray(t, r, c, "SRANDMEMBER s -20")
	if len(got) != 20 {
		t.Fatalf("SRANDMEMBER s -20 returned %d members want 20", len(got))
	}
	for _, m := range got {
		if !all[m] {
			t.Fatalf("SRANDMEMBER s -20 returned %q, not a member of the set", m)
		}
	}

	// A missing key: null without a count, empty array with one.
	if reply := sendLine(t, r, c, "SRANDMEMBER missing"); reply != "$-1" && reply != "_" {
		t.Fatalf("SRANDMEMBER missing = %q want null", reply)
	}
	if got := readArray(t, r, c, "SRANDMEMBER missing 5"); len(got) != 0 {
		t.Fatalf("SRANDMEMBER missing 5 = %v want empty array", got)
	}
}
