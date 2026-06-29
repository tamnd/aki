package command

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestHRandFieldCollIsBounded guards HRANDFIELD against the materialize trap on a
// coll-form hash. It used to route through getHash, the blob-only getter, which on
// a coll-form hash both crashed (the meta header is not a decodable blob, so
// hashDecode returned "truncated input") and, once that was sidestepped, would
// clone every field onto the heap to pick a handful: O(n) allocation for a query
// that returns a few rows, and an OOM under a tight cap on a multi-million-field
// hash. The bounded path samples through a reservoir cursor walk.
//
// The witness is allocation count for a small count: it stays far below the
// whole-hash clone a materialize would cost.
func TestHRandFieldCollIsBounded(t *testing.T) {
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	field := func(i int) []byte {
		return []byte(fmt.Sprintf("f:%08d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HSET"), []byte("h"), field(i), []byte("v")})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("h")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("hash not in coll form: OBJECT ENCODING = %q", got)
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HRANDFIELD"), []byte("h"), []byte("5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HRANDFIELD"), []byte("h"), []byte("-5")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HRANDFIELD"), []byte("h")})
	})
	// A materialize would clone all n fields, each a ~248-byte key plus value, well
	// past 8000 objects, and the cursor decode alone churned ~7 per row before the
	// forward arena. A bounded reservoir over an arena-backed walk touches only the
	// picks, a small constant driven by the count rather than the field total.
	if allocs > 600 {
		t.Fatalf("HRANDFIELD on a %d-field coll-form hash allocated %.0f objects per run; "+
			"a bounded sample should be a small constant, not O(n)", n, allocs)
	}
}

// TestHRandFieldCollMatchesBlob checks the coll-form sample returns the shapes the
// materialized path would: a single field with no count, a distinct sample for a
// positive count, a repeats-allowed sample for a negative count, the WITHVALUES
// pairing, the cap at cardinality, and the empty-key replies.
func TestHRandFieldCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	all := make(map[string]bool, n)
	for i := range n {
		f := fmt.Sprintf("f:%06d", i)
		all[f] = true
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h %s v:%06d", f, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING h"); enc != "hashtable" {
		t.Fatalf("hash encoding = %q want hashtable", enc)
	}

	// No count: a single field that belongs to the hash.
	one := bulk(t, r, c, "HRANDFIELD h")
	if !all[one] {
		t.Fatalf("HRANDFIELD h = %q, not a field of the hash", one)
	}

	// Positive count: distinct fields, all members of the hash.
	got := readArray(t, r, c, "HRANDFIELD h 10")
	if len(got) != 10 {
		t.Fatalf("HRANDFIELD h 10 returned %d fields want 10", len(got))
	}
	seen := map[string]bool{}
	for _, f := range got {
		if !all[f] {
			t.Fatalf("HRANDFIELD h 10 returned %q, not a field of the hash", f)
		}
		if seen[f] {
			t.Fatalf("HRANDFIELD h 10 returned a duplicate %q for a positive count", f)
		}
		seen[f] = true
	}

	// Count past the cardinality clamps to the whole field set, still distinct.
	got = readArray(t, r, c, "HRANDFIELD h 5000")
	if len(got) != n {
		t.Fatalf("HRANDFIELD h 5000 returned %d fields want %d", len(got), n)
	}

	// Negative count: exactly the magnitude, repeats allowed and all valid.
	got = readArray(t, r, c, "HRANDFIELD h -20")
	if len(got) != 20 {
		t.Fatalf("HRANDFIELD h -20 returned %d fields want 20", len(got))
	}
	for _, f := range got {
		if !all[f] {
			t.Fatalf("HRANDFIELD h -20 returned %q, not a field of the hash", f)
		}
	}

	// WITHVALUES: a flat field/value list in RESP2, each pair consistent.
	got = readArray(t, r, c, "HRANDFIELD h 8 WITHVALUES")
	if len(got) != 16 {
		t.Fatalf("HRANDFIELD h 8 WITHVALUES returned %d elements want 16", len(got))
	}
	for i := 0; i < len(got); i += 2 {
		f, v := got[i], got[i+1]
		if !all[f] {
			t.Fatalf("HRANDFIELD WITHVALUES field %q not a member", f)
		}
		// The seed paired f:NNNNNN with v:NNNNNN.
		if strings.TrimPrefix(f, "f:") != strings.TrimPrefix(v, "v:") {
			t.Fatalf("HRANDFIELD WITHVALUES paired %q with %q", f, v)
		}
	}

	// A missing key: null without a count, empty array with one.
	if reply := sendLine(t, r, c, "HRANDFIELD missing"); reply != "$-1" && reply != "_" {
		t.Fatalf("HRANDFIELD missing = %q want null", reply)
	}
	if got := readArray(t, r, c, "HRANDFIELD missing 5"); len(got) != 0 {
		t.Fatalf("HRANDFIELD missing 5 = %v want empty array", got)
	}
}
