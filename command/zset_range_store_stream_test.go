package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// pairsToMap turns a flat [member, score, member, score, ...] reply into a member->score
// map, the form that lets a ZRANGESTORE result be compared as a set: the destination is
// re-sorted by (score, member), so only the membership and scores matter, not the order
// the range walked.
func pairsToMap(t *testing.T, flat []string) map[string]string {
	t.Helper()
	if len(flat)%2 != 0 {
		t.Fatalf("odd WITHSCORES reply length %d: %v", len(flat), flat)
	}
	m := make(map[string]string, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		m[flat[i]] = flat[i+1]
	}
	return m
}

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestZRangeStoreCollParity checks the streamed ZRANGESTORE over a coll-form source
// stores exactly the membership the read form ZRANGE returns for the same arguments,
// across by-rank, BYSCORE and BYLEX with REV and LIMIT. The read form (bounded and
// already tested) is the oracle; the store form shares none of its result-landing code,
// so this pins the streamed window walk and the sink against it.
func TestZRangeStoreCollParity(t *testing.T) {
	r, c := startData(t)
	const n = 300 // past the 128 listpack threshold, so the source is coll form

	// src has distinct scores; lexsrc shares one score so member byte order is the rank
	// order BYLEX assumes.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD src %d m:%04d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD lexsrc 0 m:%04d", i))
	}
	for _, k := range []string{"src", "lexsrc"} {
		if enc := bulk(t, r, c, "OBJECT ENCODING "+k); enc != "skiplist" {
			t.Fatalf("zset %s not coll form: OBJECT ENCODING = %q", k, enc)
		}
	}

	type tc struct {
		src  string
		args string
	}
	cases := []tc{
		{"src", "0 -1"},
		{"src", "10 20"},
		{"src", "-5 -1"},
		{"src", "0 -1 REV"},
		{"src", "5 15 REV"},
		{"src", "100 50"}, // empty (start past stop)
		{"src", "50 150 BYSCORE"},
		{"src", "-inf +inf BYSCORE"},
		{"src", "(50 (60 BYSCORE"},
		{"src", "-inf +inf BYSCORE REV"},
		{"src", "0 +inf BYSCORE LIMIT 30 40"},
		{"src", "0 +inf BYSCORE LIMIT 30 -1"},
		{"src", "-inf +inf BYSCORE REV LIMIT 10 25"},
		{"src", "-inf +inf BYSCORE REV LIMIT 290 100"}, // offset near the end
		{"src", "-inf +inf BYSCORE LIMIT 400 10"},      // offset past the band: empty
		{"lexsrc", "[m:0100 [m:0200 BYLEX"},
		{"lexsrc", "- + BYLEX"},
		{"lexsrc", "(m:0100 (m:0110 BYLEX"},
		{"lexsrc", "- + BYLEX REV"},
		{"lexsrc", "- + BYLEX LIMIT 20 30"},
		{"lexsrc", "- + BYLEX REV LIMIT 20 30"},
		{"lexsrc", "[m:0000 [m:0050 BYLEX REV LIMIT 5 10"},
	}
	for _, k := range cases {
		// BYLEX rejects WITHSCORES, but lexsrc shares one score, so the oracle reads the
		// members alone and pins every score to that shared value.
		var want map[string]string
		if k.src == "lexsrc" {
			want = map[string]string{}
			for _, m := range readArray(t, r, c, "ZRANGE "+k.src+" "+k.args) {
				want[m] = "0"
			}
		} else {
			want = pairsToMap(t, readArray(t, r, c, "ZRANGE "+k.src+" "+k.args+" WITHSCORES"))
		}
		reply := sendLine(t, r, c, "ZRANGESTORE dst "+k.src+" "+k.args)
		if reply[0] != ':' {
			t.Fatalf("ZRANGESTORE dst %s %s failed: %q", k.src, k.args, reply)
		}
		if got := fmt.Sprintf(":%d", len(want)); reply != got {
			t.Fatalf("ZRANGESTORE dst %s %s returned %q, want count %s", k.src, k.args, reply, got)
		}
		if len(want) == 0 {
			if ex := sendLine(t, r, c, "EXISTS dst"); ex != ":0" {
				t.Fatalf("empty ZRANGESTORE dst %s %s left dst existing", k.src, k.args)
			}
			continue
		}
		got := pairsToMap(t, readArray(t, r, c, "ZRANGE dst 0 -1 WITHSCORES"))
		if !sameMap(want, got) {
			t.Fatalf("ZRANGESTORE dst %s %s\n got = %v\nwant = %v", k.src, k.args, got, want)
		}
	}
}

// TestZRangeStoreEncoding checks the destination lands in the encoding Redis would pick:
// a large result spills to the coll form (skiplist) written element by element, a small
// result buffers and writes as one listpack blob.
func TestZRangeStoreEncoding(t *testing.T) {
	r, c := startData(t)
	const n = 300
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD src %d m:%04d", i, i))
	}
	// Whole-range store: result is 300 members, past the threshold, so dst spills.
	if reply := sendLine(t, r, c, "ZRANGESTORE big src 0 -1"); reply != ":300" {
		t.Fatalf("ZRANGESTORE big = %q want :300", reply)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING big"); enc != "skiplist" {
		t.Fatalf("big encoding = %q want skiplist (spill path)", enc)
	}
	// A narrow window stays under the threshold and writes as a listpack blob.
	if reply := sendLine(t, r, c, "ZRANGESTORE small src 0 9"); reply != ":10" {
		t.Fatalf("ZRANGESTORE small = %q want :10", reply)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING small"); enc != "listpack" {
		t.Fatalf("small encoding = %q want listpack (blob path)", enc)
	}
}

// TestZRangeStoreAliasedFallback checks ZRANGESTORE where the destination is the source
// (ZRANGESTORE src src ...), which falls back to the buffered materialize because writing
// into the source mid-walk would corrupt it. The source must shrink to exactly the range.
func TestZRangeStoreAliasedFallback(t *testing.T) {
	r, c := startData(t)
	const n = 300
	want := map[string]string{}
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%04d", i, i))
		if i >= 100 && i <= 199 {
			want[fmt.Sprintf("m:%04d", i)] = fmt.Sprintf("%d", i)
		}
	}
	if reply := sendLine(t, r, c, "ZRANGESTORE z z 100 199"); reply != ":100" {
		t.Fatalf("aliased ZRANGESTORE = %q want :100", reply)
	}
	got := pairsToMap(t, readArray(t, r, c, "ZRANGE z 0 -1 WITHSCORES"))
	if !sameMap(want, got) {
		t.Fatalf("aliased ZRANGESTORE\n got = %v\nwant = %v", got, want)
	}
}

// TestZRangeStoreCollIsBounded is the OOM witness: a fixed-size window stored off a
// coll-form source must allocate the same whether the source has 4000 or 8000 members,
// because the streamed walk only touches the window's rows, never the whole source. The
// old getZSet cloned every member of the source before computing the range.
func TestZRangeStoreCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte { return []byte(fmt.Sprintf("m:%08d", i) + string(pad)) }

	build := func(size int) (*Dispatcher, *networking.Conn) {
		d := newFuzzDispatcher(t)
		conn := networking.NewOfflineConn()
		apply := func(args [][]byte) { conn.ResetOut(); d.Handle(conn, args) }
		for i := range size {
			apply([][]byte{[]byte("ZADD"), []byte("src"), []byte(fmt.Sprintf("%d", i)), member(i)})
		}
		apply([][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("src")})
		if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
			t.Fatalf("src not coll form: OBJECT ENCODING = %q", got)
		}
		return d, conn
	}
	// Store a fixed 50-member rank window; the result size is independent of the source.
	storeArgs := [][]byte{[]byte("ZRANGESTORE"), []byte("dst"), []byte("src"), []byte("0"), []byte("49")}
	measure := func(d *Dispatcher, conn *networking.Conn) float64 {
		return testing.AllocsPerRun(10, func() { conn.ResetOut(); d.Handle(conn, storeArgs) })
	}

	dSmall, cSmall := build(4000)
	small := measure(dSmall, cSmall)
	dLarge, cLarge := build(8000)
	large := measure(dLarge, cLarge)

	if large > small+300 {
		t.Fatalf("ZRANGESTORE allocated %.0f over an 8000-member source vs %.0f over 4000; "+
			"the cost must track the 50-member window, not the source", large, small)
	}
}
