package command

import (
	"bufio"
	"fmt"
	"net"
	"slices"
	"testing"

	"github.com/tamnd/aki/networking"
)

// zrangeWithScores reads dest back as a flat [member, score, ...] slice, the form the
// streamed read forms are checked against by storing and reading back through the
// same range writer.
func zrangeWithScores(t *testing.T, r *bufio.Reader, c net.Conn, key string) []string {
	t.Helper()
	return readArray(t, r, c, "ZRANGE "+key+" 0 -1 WITHSCORES")
}

// TestZSetAlgebraStreamCollParity checks the streamed ZUNION/ZINTER/ZDIFF read forms
// over coll-form sorted sets return exactly what the STORE form (still the in-RAM
// path) writes, across plain, WEIGHTS and AGGREGATE variants. Both go through the
// same score formatter, so any divergence is a real difference, not a float-format
// artifact.
func TestZSetAlgebraStreamCollParity(t *testing.T) {
	r, c := startData(t)
	const n = 300 // past the 128 listpack threshold, so both sets are coll form

	// a = members [0, n) with score = i; b = members [n/2, 3n/2) with score = i.
	// The overlap [n/2, n) drives the intersection and the difference.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d m:%04d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d m:%04d", i+n/2, i+n/2))
	}
	for _, k := range []string{"a", "b"} {
		if enc := bulk(t, r, c, "OBJECT ENCODING "+k); enc != "skiplist" {
			t.Fatalf("zset %s not coll form: OBJECT ENCODING = %q", k, enc)
		}
	}

	cases := []struct {
		read  string // the streamed read command
		store string // the STORE oracle that writes the same result to "dst"
	}{
		{"ZUNION 2 a b WITHSCORES", "ZUNIONSTORE dst 2 a b"},
		{"ZUNION 2 a b WEIGHTS 1 2 WITHSCORES", "ZUNIONSTORE dst 2 a b WEIGHTS 1 2"},
		{"ZUNION 2 a b AGGREGATE MAX WITHSCORES", "ZUNIONSTORE dst 2 a b AGGREGATE MAX"},
		{"ZUNION 2 a b AGGREGATE MIN WEIGHTS 2 3 WITHSCORES", "ZUNIONSTORE dst 2 a b WEIGHTS 2 3 AGGREGATE MIN"},
		{"ZINTER 2 a b WITHSCORES", "ZINTERSTORE dst 2 a b"},
		{"ZINTER 2 a b WEIGHTS 3 1 WITHSCORES", "ZINTERSTORE dst 2 a b WEIGHTS 3 1"},
		{"ZINTER 2 a b AGGREGATE MAX WITHSCORES", "ZINTERSTORE dst 2 a b AGGREGATE MAX"},
		{"ZDIFF 2 a b WITHSCORES", "ZDIFFSTORE dst 2 a b"},
		{"ZDIFF 2 b a WITHSCORES", "ZDIFFSTORE dst 2 b a"},
	}
	for _, tc := range cases {
		got := readArray(t, r, c, tc.read)
		if reply := sendLine(t, r, c, tc.store); reply[0] != ':' {
			t.Fatalf("%q failed: %q", tc.store, reply)
		}
		want := zrangeWithScores(t, r, c, "dst")
		if !slices.Equal(got, want) {
			t.Fatalf("%q\n got = %v\nwant = %v (from %q)", tc.read, got, want, tc.store)
		}
	}
}

// TestZSetAlgebraStreamMixedForms checks the streamed algebra still matches the
// store oracle when one source is a small blob and the other is coll form, the path
// that maps the blob once and point-probes or streams the coll source.
func TestZSetAlgebraStreamMixedForms(t *testing.T) {
	r, c := startData(t)
	const n = 300
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD big %d m:%04d", i, i))
	}
	// A small blob zset overlapping the tail of big.
	for i := n - 5; i < n+5; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD small %d m:%04d", i*2, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING big"); enc != "skiplist" {
		t.Fatalf("big encoding = %q want skiplist", enc)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING small"); enc != "listpack" {
		t.Fatalf("small encoding = %q want listpack", enc)
	}

	for _, pair := range [][2]string{
		{"ZUNION 2 big small WITHSCORES", "ZUNIONSTORE dst 2 big small"},
		{"ZINTER 2 big small WITHSCORES", "ZINTERSTORE dst 2 big small"},
		{"ZINTER 2 small big WITHSCORES", "ZINTERSTORE dst 2 small big"},
		{"ZDIFF 2 big small WITHSCORES", "ZDIFFSTORE dst 2 big small"},
		{"ZDIFF 2 small big WITHSCORES", "ZDIFFSTORE dst 2 small big"},
	} {
		got := readArray(t, r, c, pair[0])
		_ = sendLine(t, r, c, pair[1])
		want := zrangeWithScores(t, r, c, "dst")
		if !slices.Equal(got, want) {
			t.Fatalf("%q\n got = %v\nwant = %v", pair[0], got, want)
		}
	}
}

// TestZInterStreamCollIsBounded is the OOM witness for the read form: ZINTER drives
// the smallest source and point-probes the rest, so the per-call allocation must not
// grow when a non-driver source grows. The old loadZSets cloned every member of
// every source, so the cost scaled with the largest set; the streamed path is
// independent of it.
func TestZInterStreamCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte { return []byte(fmt.Sprintf("m:%08d", i) + string(pad)) }

	// Driver "a" is a fixed 200-member coll zset; "b" is the large probed source,
	// built at two sizes. Every member of a is in b, so the result is a's 200.
	build := func(bSize int) (*Dispatcher, *networking.Conn) {
		d := newFuzzDispatcher(t)
		conn := networking.NewOfflineConn()
		apply := func(args [][]byte) { conn.ResetOut(); d.Handle(conn, args) }
		for i := range 200 {
			apply([][]byte{[]byte("ZADD"), []byte("a"), []byte("1"), member(i)})
		}
		for i := range bSize {
			apply([][]byte{[]byte("ZADD"), []byte("b"), []byte("1"), member(i)})
		}
		for _, k := range []string{"a", "b"} {
			apply([][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte(k)})
			if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
				t.Fatalf("zset %s not coll form: OBJECT ENCODING = %q", k, got)
			}
		}
		return d, conn
	}
	interArgs := [][]byte{[]byte("ZINTER"), []byte("2"), []byte("a"), []byte("b")}
	measure := func(d *Dispatcher, conn *networking.Conn) float64 {
		return testing.AllocsPerRun(10, func() { conn.ResetOut(); d.Handle(conn, interArgs) })
	}

	dSmall, cSmall := build(4000)
	small := measure(dSmall, cSmall)
	dLarge, cLarge := build(8000)
	large := measure(dLarge, cLarge)

	// Doubling the probed source must not move the per-call cost: the driver is
	// fixed at 200 and b is reached by point lookups, never cloned.
	if large > small+300 {
		t.Fatalf("ZINTER allocated %.0f over an 8000-member b vs %.0f over 4000; "+
			"the cost must track the 200-member driver, not the probed source", large, small)
	}
}
