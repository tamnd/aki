package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSISMEMBERLargeSetIsPointLookup guards the membership path against a
// regression where SISMEMBER materialized the whole set and scanned it. On a
// btree-backed set that walk is O(n) in both time and allocation: it clones
// every member on every call, so a multi-million-member set drags its entire
// contents through the heap on each probe and a tight memory cap OOM-kills the
// server mid-benchmark. The fix answers membership with a sub-tree point lookup.
//
// The witness is allocation count. A point lookup touches a fixed handful of
// objects no matter how big the set is; the old whole-set scan allocated one
// clone per member. We build a set far past the listpack threshold, with members
// long enough that a scan would be unmistakably expensive, then assert a single
// SISMEMBER allocates a small constant well under the member count.
func TestSISMEMBERLargeSetIsPointLookup(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		// Entropy at the front so the set is genuinely diverse, then padding so a
		// whole-set clone would move ~1MB per call.
		return []byte(fmt.Sprintf("%08d", i) + string(pad))
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

	present := member(1234)
	absent := append([]byte("zzzz"), pad...)

	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SISMEMBER"), []byte("s"), present})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SISMEMBER"), []byte("s"), absent})
	})
	// Two point lookups per run. A whole-set scan would allocate on the order of
	// 2*n clones (8000); a point lookup is a small constant. Bound it well below
	// the member count so the O(n) path can never sneak back in.
	if allocs > 200 {
		t.Fatalf("SISMEMBER on a %d-member set allocated %.0f objects per run; "+
			"the point-lookup path should be a small constant, not O(n)", n, allocs)
	}

	// Correctness still holds on the point-lookup path.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("SISMEMBER"), []byte("s"), present})
	if got := string(conn.OutBytes()); got != ":1\r\n" {
		t.Fatalf("SISMEMBER present = %q want :1", got)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("SISMEMBER"), []byte("s"), absent})
	if got := string(conn.OutBytes()); got != ":0\r\n" {
		t.Fatalf("SISMEMBER absent = %q want :0", got)
	}
}
