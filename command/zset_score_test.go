package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZScoreLargeZSetIsPointLookup guards the score path against the same regression
// that hit SISMEMBER: ZSCORE and ZMSCORE ran through getZSet, which materializes every
// member of the sorted set and then linear-scans it. On a btree-backed (skiplist-coll)
// sorted set that walk is O(n) in both time and allocation, so a multi-million-member
// set drags its whole contents through the heap on each score probe and a tight memory
// cap OOM-kills the server mid-benchmark. The fix answers a score with a point lookup on
// the member-index row.
//
// The witness is allocation count. A point lookup touches a fixed handful of objects no
// matter how big the set is; the old whole-set scan allocated one clone per member. We
// build a sorted set far past the listpack threshold, with members long enough that a
// scan would be unmistakably expensive, then assert a ZSCORE plus a ZMSCORE allocate a
// small constant well under the member count.
func TestZScoreLargeZSetIsPointLookup(t *testing.T) {
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
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte(fmt.Sprintf("%d", i)), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	present := member(1234)
	absent := append([]byte("zzzz"), pad...)

	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZSCORE"), []byte("z"), present})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZMSCORE"), []byte("z"), present, absent})
	})
	// One ZSCORE plus a two-member ZMSCORE per run. A whole-set scan would allocate on
	// the order of 3*n clones (12000); point lookups are a small constant. Bound it well
	// below the member count so the O(n) path can never sneak back in.
	if allocs > 200 {
		t.Fatalf("ZSCORE/ZMSCORE on a %d-member sorted set allocated %.0f objects per run; "+
			"the point-lookup path should be a small constant, not O(n)", n, allocs)
	}

	// Correctness still holds on the point-lookup path.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("ZSCORE"), []byte("z"), present})
	if got := string(conn.OutBytes()); got != "$4\r\n1234\r\n" {
		t.Fatalf("ZSCORE present = %q want score 1234", got)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("ZSCORE"), []byte("z"), absent})
	if got := string(conn.OutBytes()); got != "$-1\r\n" {
		t.Fatalf("ZSCORE absent = %q want nil", got)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("ZMSCORE"), []byte("z"), present, absent})
	if got := string(conn.OutBytes()); got != "*2\r\n$4\r\n1234\r\n$-1\r\n" {
		t.Fatalf("ZMSCORE = %q want [1234, nil]", got)
	}
}
