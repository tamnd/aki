package command

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestZSetAlgebraStoreParity checks the streamed STORE forms write exactly what the
// streamed read form returns, across plain, WEIGHTS and AGGREGATE variants over two
// coll-form sorted sets, and that the destination ends up coll form (the spill path
// ran). The read form and the sink share the same algebra core but diverge entirely
// in how the result lands (a collected slice versus a buffered-then-spilled sub-tree
// with a read-modify-write aggregate), so this pins the sink against the collector.
func TestZSetAlgebraStoreParity(t *testing.T) {
	r, c := startData(t)
	const n = 300 // past the 128 listpack threshold, so the sources and result are coll form

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d m:%04d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d m:%04d", i+n/2, i+n/2))
	}

	cases := []struct {
		read  string // the streamed read command
		store string // the STORE form that must write the same result to dst
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
		want := readArray(t, r, c, tc.read)
		reply := sendLine(t, r, c, tc.store)
		if reply[0] != ':' {
			t.Fatalf("%q failed: %q", tc.store, reply)
		}
		// The stored cardinality must match the read form's member count.
		if got := strconv.Itoa(len(want) / 2); reply[1:] != got {
			t.Fatalf("%q returned %q, want count %s", tc.store, reply, got)
		}
		got := zrangeWithScores(t, r, c, "dst")
		if !slices.Equal(got, want) {
			t.Fatalf("%q\n got = %v\nwant = %v (from %q)", tc.store, got, want, tc.read)
		}
		if enc := bulk(t, r, c, "OBJECT ENCODING dst"); enc != "skiplist" {
			t.Fatalf("%q left dst encoding %q, want skiplist (spill path)", tc.store, enc)
		}
	}
}

// TestZSetAlgebraStoreBlobParity checks the common small-result path: the sink buffers
// the whole result and writes it as one listpack blob, exactly the form Redis picks,
// and the result matches the read form.
func TestZSetAlgebraStoreBlobParity(t *testing.T) {
	r, c := startData(t)
	// Small sources stay listpack and the union of them stays under the threshold.
	for i := range 10 {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d m:%02d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d m:%02d", i+5, i+5))
	}
	for _, tc := range []struct{ read, store string }{
		{"ZUNION 2 a b WITHSCORES", "ZUNIONSTORE dst 2 a b"},
		{"ZINTER 2 a b WITHSCORES", "ZINTERSTORE dst 2 a b"},
		{"ZDIFF 2 a b WITHSCORES", "ZDIFFSTORE dst 2 a b"},
	} {
		want := readArray(t, r, c, tc.read)
		_ = sendLine(t, r, c, tc.store)
		got := zrangeWithScores(t, r, c, "dst")
		if !slices.Equal(got, want) {
			t.Fatalf("%q\n got = %v\nwant = %v", tc.store, got, want)
		}
		if enc := bulk(t, r, c, "OBJECT ENCODING dst"); enc != "listpack" {
			t.Fatalf("%q left dst encoding %q, want listpack (blob path)", tc.store, enc)
		}
	}
}

// TestZUnionStoreAggregatesIndependently checks the ZUNIONSTORE tree read-modify-write
// against a hand-computed oracle, not against the production algebra. A member in both
// sources has its score combined under the AGGREGATE mode in the spilled sub-tree
// (read the stored score, combine, rewrite the moved score row), the novel code in
// this slice, so it is checked against scores computed independently in the test.
func TestZUnionStoreAggregatesIndependently(t *testing.T) {
	r, c := startData(t)
	// a = m:0000..m:0199 score i; b = m:0100..m:0299 score 10*i. Overlap m:0100..m:0199.
	aMap := map[string]float64{}
	bMap := map[string]float64{}
	for i := range 200 {
		m := fmt.Sprintf("m:%04d", i)
		aMap[m] = float64(i)
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d %s", i, m))
	}
	for i := 100; i < 300; i++ {
		m := fmt.Sprintf("m:%04d", i)
		bMap[m] = float64(10 * i)
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d %s", 10*i, m))
	}

	combine := func(agg string, x, y float64) float64 {
		switch agg {
		case "MIN":
			return min(x, y)
		case "MAX":
			return max(x, y)
		default:
			return x + y
		}
	}
	for _, tc := range []struct {
		agg    string
		wa, wb float64
	}{
		{"SUM", 2, 3},
		{"MIN", 2, 3},
		{"MAX", 2, 3},
		{"SUM", 1, 1},
	} {
		// Independent expected map: weighted score per source, combined on overlap.
		want := map[string]float64{}
		for m, s := range aMap {
			want[m] = tc.wa * s
		}
		for m, s := range bMap {
			ws := tc.wb * s
			if cur, ok := want[m]; ok {
				want[m] = combine(tc.agg, cur, ws)
			} else {
				want[m] = ws
			}
		}

		store := fmt.Sprintf("ZUNIONSTORE dst 2 a b WEIGHTS %g %g AGGREGATE %s", tc.wa, tc.wb, tc.agg)
		_ = sendLine(t, r, c, store)
		got := zrangeWithScores(t, r, c, "dst")
		if len(got) != 2*len(want) {
			t.Fatalf("%q stored %d members, want %d", store, len(got)/2, len(want))
		}
		for i := 0; i < len(got); i += 2 {
			m, scoreStr := got[i], got[i+1]
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				t.Fatalf("bad score %q: %v", scoreStr, err)
			}
			if w, ok := want[m]; !ok || w != score {
				t.Fatalf("%q member %s score %v, want %v (present=%v)", store, m, score, w, ok)
			}
		}
	}
}

// TestZSetStoreAliasedFallback checks the case where the destination is also a source
// (ZUNIONSTORE a 2 a b). Writing into a source mid-walk is unsafe, so this falls back
// to the buffered compute; the result must still be correct.
func TestZSetStoreAliasedFallback(t *testing.T) {
	r, c := startData(t)
	for i := range 200 {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d m:%04d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d m:%04d", i+100, i+100))
	}
	// Independent expected: union of a and b with default weights and SUM.
	want := map[string]float64{}
	for i := range 200 {
		want[fmt.Sprintf("m:%04d", i)] = float64(i)
	}
	for i := range 200 {
		m := fmt.Sprintf("m:%04d", i+100)
		if cur, ok := want[m]; ok {
			want[m] = cur + float64(i+100)
		} else {
			want[m] = float64(i + 100)
		}
	}
	_ = sendLine(t, r, c, "ZUNIONSTORE a 2 a b") // destination a aliases a source
	got := zrangeWithScores(t, r, c, "a")
	if len(got) != 2*len(want) {
		t.Fatalf("aliased ZUNIONSTORE stored %d members, want %d", len(got)/2, len(want))
	}
	for i := 0; i < len(got); i += 2 {
		score, _ := strconv.ParseFloat(got[i+1], 64)
		if w, ok := want[got[i]]; !ok || w != score {
			t.Fatalf("aliased member %s score %v, want %v (present=%v)", got[i], score, w, ok)
		}
	}
}

// TestZInterStoreCollIsBounded is the OOM witness for the STORE form: ZINTERSTORE
// drives the smallest source and point-probes the rest, writing the bounded result
// into the destination, so the per-call allocation must not grow when a non-driver
// source grows. The old loadZSets cloned every member of every source.
func TestZInterStoreCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte { return []byte(fmt.Sprintf("m:%08d", i) + string(pad)) }

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
	storeArgs := [][]byte{[]byte("ZINTERSTORE"), []byte("dst"), []byte("2"), []byte("a"), []byte("b")}
	measure := func(d *Dispatcher, conn *networking.Conn) float64 {
		return testing.AllocsPerRun(10, func() { conn.ResetOut(); d.Handle(conn, storeArgs) })
	}

	dSmall, cSmall := build(4000)
	small := measure(dSmall, cSmall)
	dLarge, cLarge := build(8000)
	large := measure(dLarge, cLarge)

	if large > small+300 {
		t.Fatalf("ZINTERSTORE allocated %.0f over an 8000-member b vs %.0f over 4000; "+
			"the cost must track the 200-member driver, not the probed source", large, small)
	}
}
