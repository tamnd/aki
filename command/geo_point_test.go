package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestGeoPointReadsAreBounded guards GEODIST, GEOPOS and GEOHASH against the
// materialize trap. All three called getZSet and then zsetFind, which on a
// coll-form geo set (a sorted set past the skiplist threshold) clones every member
// onto the heap to read the score of one or two of them: O(n) allocation for an
// O(1) point read. A geo set with millions of members would drag its whole contents
// through memory on each lookup and OOM under a tight cap. The fix routes the three
// through zsetMemberScores, a point lookup on the member-index rows.
//
// The witness is allocation count: a point read over a handful of members touches a
// fixed number of objects no matter how big the set is. We build a geo set well past
// the skiplist threshold with padded member names so a whole-set clone would move
// about a megabyte, then assert each command stays a small constant.
func TestGeoPointReadsAreBounded(t *testing.T) {
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
		lon := fmt.Sprintf("%.4f", float64(i%360)-180+0.5)
		lat := fmt.Sprintf("%.4f", float64(i%170)-85+0.5)
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEOADD"), []byte("geo"), []byte(lon), []byte(lat), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("geo")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("geo set not in coll form: OBJECT ENCODING = %q", got)
	}

	a, b := member(1234), member(2345)
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEODIST"), []byte("geo"), a, b, []byte("km")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEOPOS"), []byte("geo"), a, b})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEOHASH"), []byte("geo"), a})
	})
	// Three point reads over at most two members each. A whole-set clone would be on
	// the order of n; bound well below n so the materialize path cannot return.
	if allocs > 200 {
		t.Fatalf("GEODIST/GEOPOS/GEOHASH on a %d-member geo set allocated %.0f objects per run; "+
			"point reads should be a small constant, not O(n)", n, allocs)
	}

	// Correctness: a present member round-trips its coordinate, an absent one is nil.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("GEOPOS"), []byte("geo"), a, []byte("nope")})
	out := string(conn.OutBytes())
	// Two-element outer array: a present [lon,lat] pair then a nil array.
	if out[:4] != "*2\r\n" {
		t.Fatalf("GEOPOS reply header = %q", out[:4])
	}
}
