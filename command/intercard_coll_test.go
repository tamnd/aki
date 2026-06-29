package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSInterCardCollIsBounded guards SINTERCARD against the materialize trap. The
// old path called loadSets, cloning every member of every input set onto the heap
// before a single intersection was counted, so a SINTERCARD with a small LIMIT over
// two multi-million-member sets still dragged both through memory and OOM-killed
// under a tight cap. The bounded path reads each cardinality in O(1), drives the
// smallest set in copied batches, point-probes the rest, and stops at the LIMIT.
//
// The witness is allocation count with LIMIT 1: a full-overlap intersection answers
// after the first batch, so the count stays a small constant no matter how big the
// sets are. A materialize would clone all 2n members first.
func TestSInterCardCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
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
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("a"), member(i)})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("b"), member(i)})
	}
	for _, k := range []string{"a", "b"} {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte(k)})
		if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
			t.Fatalf("set %s not in coll form: OBJECT ENCODING = %q", k, got)
		}
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SINTERCARD"), []byte("2"), []byte("a"), []byte("b"), []byte("LIMIT"), []byte("1")})
	})
	// One batch drives, one probe answers, the LIMIT stops the walk. A materialize
	// would clone all 2n (8000) members first; the bounded path is a single batch.
	if allocs > 2000 {
		t.Fatalf("SINTERCARD LIMIT 1 over two %d-member coll sets allocated %.0f objects per run; "+
			"the bounded path should stop after the first batch, not clone 2n", n, allocs)
	}
}

// TestSInterCardCollMatchesNaive checks the bounded SINTERCARD returns the count a
// straightforward intersection would: a partial overlap, the no-overlap zero, the
// LIMIT clamp, a mixed blob+coll pairing, and the missing-key zero.
func TestSInterCardCollMatchesNaive(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	// a = [0, n), b = [n/2, 3n/2): overlap is [n/2, n) = n/2 members. Both coll form.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD a m:%06d", i))
		_ = sendLine(t, r, c, fmt.Sprintf("SADD b m:%06d", i+n/2))
	}
	for _, k := range []string{"a", "b"} {
		if enc := bulk(t, r, c, "OBJECT ENCODING "+k); enc != "hashtable" {
			t.Fatalf("set %s encoding = %q want hashtable", k, enc)
		}
	}

	if got := sendLine(t, r, c, "SINTERCARD 2 a b"); got != fmt.Sprintf(":%d", n/2) {
		t.Fatalf("SINTERCARD 2 a b = %q want :%d", got, n/2)
	}
	// LIMIT clamps the answer.
	if got := sendLine(t, r, c, "SINTERCARD 2 a b LIMIT 10"); got != ":10" {
		t.Fatalf("SINTERCARD 2 a b LIMIT 10 = %q want :10", got)
	}
	// LIMIT above the true count returns the true count.
	if got := sendLine(t, r, c, "SINTERCARD 2 a b LIMIT 100000"); got != fmt.Sprintf(":%d", n/2) {
		t.Fatalf("SINTERCARD LIMIT past count = %q want :%d", got, n/2)
	}

	// A small blob set fully inside the overlap: intersection is its own size.
	for i := n / 2; i < n/2+8; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD small m:%06d", i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING small"); enc != "listpack" && enc != "intset" {
		t.Fatalf("small set encoding = %q want a blob form", enc)
	}
	if got := sendLine(t, r, c, "SINTERCARD 3 a b small"); got != ":8" {
		t.Fatalf("SINTERCARD 3 a b small = %q want :8", got)
	}

	// No overlap is zero.
	for i := range 5 {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD far z:%06d", i))
	}
	if got := sendLine(t, r, c, "SINTERCARD 2 a far"); got != ":0" {
		t.Fatalf("SINTERCARD disjoint = %q want :0", got)
	}
	// A missing key makes the intersection empty.
	if got := sendLine(t, r, c, "SINTERCARD 2 a missing"); got != ":0" {
		t.Fatalf("SINTERCARD with missing key = %q want :0", got)
	}
}

// TestZInterCardCollIsBounded is the sorted-set mirror of the SINTERCARD witness.
// ZINTERCARD used to call loadZSets (getZSet clones every pair), so the LIMIT could
// not save a huge intersection from materializing first. The bounded path walks the
// member-index rows of the smallest in batches and point-probes the rest.
func TestZInterCardCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
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
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("a"), []byte("1"), member(i)})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("b"), []byte("1"), member(i)})
	}
	for _, k := range []string{"a", "b"} {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte(k)})
		if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
			t.Fatalf("zset %s not in coll form: OBJECT ENCODING = %q", k, got)
		}
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZINTERCARD"), []byte("2"), []byte("a"), []byte("b"), []byte("LIMIT"), []byte("1")})
	})
	if allocs > 2000 {
		t.Fatalf("ZINTERCARD LIMIT 1 over two %d-member coll zsets allocated %.0f objects per run; "+
			"the bounded path should stop after the first batch, not clone 2n", n, allocs)
	}
}

// TestZInterCardCollMatchesNaive mirrors the set equivalence test over sorted sets:
// scores are irrelevant to the cardinality, so only membership decides the count.
func TestZInterCardCollMatchesNaive(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD a %d m:%06d", i, i))
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD b %d m:%06d", i, i+n/2))
	}
	for _, k := range []string{"a", "b"} {
		if enc := bulk(t, r, c, "OBJECT ENCODING "+k); enc != "skiplist" {
			t.Fatalf("zset %s encoding = %q want skiplist", k, enc)
		}
	}

	if got := sendLine(t, r, c, "ZINTERCARD 2 a b"); got != fmt.Sprintf(":%d", n/2) {
		t.Fatalf("ZINTERCARD 2 a b = %q want :%d", got, n/2)
	}
	if got := sendLine(t, r, c, "ZINTERCARD 2 a b LIMIT 10"); got != ":10" {
		t.Fatalf("ZINTERCARD 2 a b LIMIT 10 = %q want :10", got)
	}

	// A small blob zset inside the overlap.
	for i := n / 2; i < n/2+8; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD small %d m:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING small"); enc != "listpack" {
		t.Fatalf("small zset encoding = %q want listpack", enc)
	}
	if got := sendLine(t, r, c, "ZINTERCARD 3 a b small"); got != ":8" {
		t.Fatalf("ZINTERCARD 3 a b small = %q want :8", got)
	}

	if got := sendLine(t, r, c, "ZINTERCARD 2 a missing"); got != ":0" {
		t.Fatalf("ZINTERCARD with missing key = %q want :0", got)
	}
}
