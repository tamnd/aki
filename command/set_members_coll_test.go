package command

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSMembersCollIsBounded guards SMEMBERS against the full-reply materialize trap
// on a coll-form set. It used to route through getSet, which clones every member
// onto the heap as a [][]byte before writing the reply: O(n) transient heap on top
// of the O(n) reply bytes, and an OOM under a tight cap on a set larger than RAM.
// The streaming path writes each member straight from a sub-tree cursor into the
// encoder, so retained working memory is one cursor page plus the flush buffer,
// never a whole-set clone.
//
// The witness is allocation count: with the output buffer reused across runs, the
// streaming walk allocates a small constant, far below the per-run [][]byte a
// materialize would cost.
func TestSMembersCollIsBounded(t *testing.T) {
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
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("s"), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("s")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("set not in coll form: OBJECT ENCODING = %q", got)
	}

	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SMEMBERS"), []byte("s")})
	})
	// A materialize clones all n members into a [][]byte every run, well past 4000
	// objects. The streaming walk writes each member into the reused output buffer
	// from a page-aliased cursor, so the only per-run allocations are the cursor and
	// reader setup, a small constant independent of the member total.
	if allocs > 50 {
		t.Fatalf("SMEMBERS on a %d-member coll-form set allocated %.0f objects per run; "+
			"a streamed reply should be a small constant, not O(n)", n, allocs)
	}
}

// TestSMembersCollMatchesBlob checks the streamed coll-form reply carries exactly
// the members the materialized path would, with the right set length, and that the
// empty-key reply stays an empty set.
func TestSMembersCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	want := make([]string, 0, n)
	for i := range n {
		m := fmt.Sprintf("m:%06d", i)
		want = append(want, m)
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s %s", m))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "hashtable" {
		t.Fatalf("set encoding = %q want hashtable", enc)
	}

	got := readArray(t, r, c, "SMEMBERS s")
	if len(got) != n {
		t.Fatalf("SMEMBERS s returned %d members want %d", len(got), n)
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SMEMBERS s member %d = %q want %q", i, got[i], want[i])
		}
	}

	// A missing key is an empty set, not an error.
	if got := readArray(t, r, c, "SMEMBERS missing"); len(got) != 0 {
		t.Fatalf("SMEMBERS missing = %v want empty set", got)
	}

	// A wrong-type key still reports WRONGTYPE.
	_ = sendLine(t, r, c, "SET str hello")
	if reply := sendLine(t, r, c, "SMEMBERS str"); reply[:1] != "-" {
		t.Fatalf("SMEMBERS on a string = %q want WRONGTYPE error", reply)
	}
}

// TestSMembersCollStreamsLargeReply drives a coll-form set whose reply is well past
// the mid-reply flush threshold over a real connection, so StreamFlush actually
// spills the buffer to the socket several times during the single command. The
// client must still reassemble the exact member set, which checks the partial
// flushes carry clean RESP framing across the chunk boundaries.
func TestSMembersCollStreamsLargeReply(t *testing.T) {
	r, c := startData(t)
	// 1000 members at ~256 bytes each is about a quarter megabyte, several times the
	// 64 KiB flush threshold, so the reply leaves the server in multiple socket
	// writes rather than one.
	const n = 1000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	want := make(map[string]bool, n)
	for i := range n {
		m := fmt.Sprintf("m:%06d:%s", i, pad)
		want[m] = true
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s %s", m))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "hashtable" {
		t.Fatalf("set encoding = %q want hashtable", enc)
	}

	got := readArray(t, r, c, "SMEMBERS s")
	if len(got) != n {
		t.Fatalf("SMEMBERS s returned %d members want %d", len(got), n)
	}
	for _, m := range got {
		if !want[m] {
			t.Fatalf("SMEMBERS s returned %q, not a member of the set", m)
		}
		delete(want, m)
	}
	if len(want) != 0 {
		t.Fatalf("SMEMBERS s dropped %d members across the streamed reply", len(want))
	}
}
