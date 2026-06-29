package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestGeoSearchCollIsBounded guards GEOSEARCH against the materialize trap on a
// coll-form geo set. A geo set is stored as a sorted set keyed by the geohash
// score, and GEOSEARCH used to call getZSet, cloning every member onto the heap
// before testing the shape. A radius query that matches a handful of points still
// dragged a multi-million-member geo set through memory, an OOM under a tight cap.
// The bounded path streams the member index through an arena cursor and keeps only
// the matches.
//
// The witness is allocation count for a small-radius query over a large set: the
// streamed path copies only the few matches, so it stays a small constant while a
// materialize would clone all n members.
func TestGeoSearchCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 2000
	// Far points sit on the southern parallel, thousands of km from the center.
	for i := range n {
		lon := -180.0 + 360.0*float64(i)/float64(n)
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEOADD"), []byte("g"),
			[]byte(fmt.Sprintf("%.6f", lon)), []byte("-60"), []byte(fmt.Sprintf("f:%05d", i))})
	}
	// A small cluster within ~12 km of (15, 37).
	near := [][2]float64{{15.0, 37.0}, {15.1, 37.0}, {14.9, 37.0}, {15.0, 37.1}, {15.0, 36.9}}
	for i, p := range near {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("GEOADD"), []byte("g"),
			[]byte(fmt.Sprintf("%.6f", p[0])), []byte(fmt.Sprintf("%.6f", p[1])), []byte(fmt.Sprintf("n:%d", i))})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("g")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("geo set not in coll form: OBJECT ENCODING = %q", got)
	}

	args := [][]byte{[]byte("GEOSEARCH"), []byte("g"), []byte("FROMLONLAT"),
		[]byte("15"), []byte("37"), []byte("BYRADIUS"), []byte("50"), []byte("km"), []byte("ASC")}
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, args)
	})
	// Five matches copied plus the reply; a materialize would clone all n members.
	if allocs > 600 {
		t.Fatalf("GEOSEARCH over a %d-member coll geo set allocated %.0f objects per run; "+
			"the streamed path should copy only the matches, not clone n", n, allocs)
	}
}

// TestGeoSearchCollMatchesNaive checks the streamed coll-form search returns what
// the old whole-set scan would: the right members for a radius, the COUNT cap to
// the nearest, a FROMMEMBER center, the ANY early-exit count, and GEOSEARCHSTORE.
func TestGeoSearchCollMatchesNaive(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	for i := range n {
		lon := -180.0 + 360.0*float64(i)/float64(n)
		_ = sendLine(t, r, c, fmt.Sprintf("GEOADD g %.6f -60 f:%05d", lon, i))
	}
	near := [][2]float64{{15.0, 37.0}, {15.1, 37.0}, {14.9, 37.0}, {15.0, 37.1}, {15.0, 36.9}}
	for i, p := range near {
		_ = sendLine(t, r, c, fmt.Sprintf("GEOADD g %.6f %.6f n:%d", p[0], p[1], i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING g"); enc != "skiplist" {
		t.Fatalf("geo set encoding = %q want skiplist", enc)
	}

	// A 50 km radius around the cluster returns exactly the five near members.
	got := readArray(t, r, c, "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 50 km ASC")
	if len(got) != len(near) {
		t.Fatalf("GEOSEARCH radius returned %d members want %d: %v", len(got), len(near), got)
	}
	for _, m := range got {
		if len(m) < 2 || m[0] != 'n' {
			t.Fatalf("GEOSEARCH returned a far member %q inside the cluster radius", m)
		}
	}
	// Nearest first: the cluster center is closest.
	if got[0] != "n:0" {
		t.Fatalf("GEOSEARCH ASC first = %q want n:0 (the center)", got[0])
	}

	// COUNT caps to the nearest matches.
	got = readArray(t, r, c, "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 50 km ASC COUNT 2")
	if len(got) != 2 || got[0] != "n:0" {
		t.Fatalf("GEOSEARCH COUNT 2 = %v want 2 nearest starting n:0", got)
	}

	// COUNT ANY stops early but still returns COUNT members, all from the cluster.
	got = readArray(t, r, c, "GEOSEARCH g FROMLONLAT 15 37 BYRADIUS 50 km COUNT 3 ANY")
	if len(got) != 3 {
		t.Fatalf("GEOSEARCH COUNT 3 ANY returned %d want 3", len(got))
	}
	for _, m := range got {
		if len(m) < 1 || m[0] != 'n' {
			t.Fatalf("GEOSEARCH ANY returned a far member %q", m)
		}
	}

	// FROMMEMBER centers on a member by point lookup.
	got = readArray(t, r, c, "GEOSEARCH g FROMMEMBER n:0 BYRADIUS 50 km ASC")
	if len(got) != len(near) || got[0] != "n:0" {
		t.Fatalf("GEOSEARCH FROMMEMBER = %v want the cluster with n:0 first", got)
	}

	// GEOSEARCHSTORE writes the matches to a destination sorted set.
	if got := sendLine(t, r, c, "GEOSEARCHSTORE dst g FROMLONLAT 15 37 BYRADIUS 50 km ASC"); got != fmt.Sprintf(":%d", len(near)) {
		t.Fatalf("GEOSEARCHSTORE = %q want :%d", got, len(near))
	}
	if got := sendLine(t, r, c, "ZCARD dst"); got != fmt.Sprintf(":%d", len(near)) {
		t.Fatalf("ZCARD dst = %q want :%d", got, len(near))
	}
}
