package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestHGetLargeHashIsPointLookup guards HGET against the materialize trap on a
// coll-form hash. The full-reply commands (HGETALL/HKEYS/HVALS) already have a
// bound in TestHashFullCollIsBounded, but HGET is the hot point read and needs its
// own standing alloc gate so the whole DESCENT-RISK group is covered uniformly
// (spec 2064/ltm/07 section 4): a regression that routed HGET back through a
// whole-hash clone to fetch one field would be O(n) allocation per probe and OOM a
// hash larger than RAM under a tight cap. The bounded path point-looks up the one
// field on the sub-tree.
//
// The witness is allocation count: a field fetch touches a fixed handful of objects
// no matter how big the hash is. We build a hash past the listpack threshold with
// padded values so a clone would be unmistakably expensive, then assert HGET of a
// present and an absent field allocates a small constant well under the field count.
func TestHGetLargeHashIsPointLookup(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	field := func(i int) []byte {
		return []byte(fmt.Sprintf("f:%08d", i))
	}
	value := func(i int) []byte {
		// Padded so a whole-hash clone would move ~1MB per call.
		return []byte(fmt.Sprintf("%08d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HSET"), []byte("h"), field(i), value(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("h")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("hash not in coll form: OBJECT ENCODING = %q", got)
	}

	present := field(1234)
	absent := []byte("f:nope")

	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HGET"), []byte("h"), present})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HGET"), []byte("h"), absent})
	})
	// Two point lookups per run. A whole-hash clone would allocate on the order of
	// n field+value copies; a point lookup is a small constant. Bound it well below
	// the field count so the O(n) path can never sneak back in.
	if allocs > 200 {
		t.Fatalf("HGET on a %d-field hash allocated %.0f objects per run; "+
			"the point-lookup path should be a small constant, not O(n)", n, allocs)
	}

	// Correctness still holds on the point-lookup path.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HGET"), []byte("h"), present})
	want := "$248\r\n" + fmt.Sprintf("%08d", 1234) + string(pad) + "\r\n"
	if got := string(conn.OutBytes()); got != want {
		t.Fatalf("HGET present = %q want %q", got, want)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HGET"), []byte("h"), absent})
	if got := string(conn.OutBytes()); got != "$-1\r\n" {
		t.Fatalf("HGET absent = %q want $-1 (nil)", got)
	}
}
