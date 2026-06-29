package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/tamnd/aki/networking"
)

// aggScanAll drives a per-key scan (HSCAN/SSCAN/ZSCAN) to completion, following
// the cursor token each page returns until it comes back "0", and returns every
// flat reply element across all pages. It is the client-side contract the coll-form
// cursor path must satisfy: a resumable scan that terminates.
func aggScanAll(t *testing.T, r *bufio.Reader, c net.Conn, cmd, key, tail string) []string {
	t.Helper()
	var all []string
	cursor := "0"
	for i := 0; ; i++ {
		next, flat := scanReply(t, r, c, fmt.Sprintf("%s %s %s%s", cmd, key, cursor, tail))
		all = append(all, flat...)
		if next == "0" {
			break
		}
		cursor = next
		if i > 100000 {
			t.Fatalf("%s did not terminate", cmd)
		}
	}
	return all
}

// TestAggScanCollFullCoverage checks that HSCAN/SSCAN/ZSCAN over a coll-form
// collection visit every element exactly once across the paged cursor walk, rather
// than dumping the whole collection in one materialized reply. The old handlers
// called getHash/getSet/getZSet and returned everything with a "0" cursor, which on
// a coll-form collection clones every element onto the heap; the cursor path reads
// COUNT rows per page and resumes from an opaque sub-key token.
func TestAggScanCollFullCoverage(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	// Hash: build well past the hashtable threshold, scan with a small COUNT so the
	// walk genuinely pages.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h field:%06d val:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING h"); enc != "hashtable" {
		t.Fatalf("hash encoding = %q want hashtable", enc)
	}
	flat := aggScanAll(t, r, c, "HSCAN", "h", " COUNT 16")
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	if len(got) != n {
		t.Fatalf("HSCAN covered %d fields want %d", len(got), n)
	}
	for i := range n {
		f := fmt.Sprintf("field:%06d", i)
		if got[f] != fmt.Sprintf("val:%06d", i) {
			t.Fatalf("HSCAN field %s = %q", f, got[f])
		}
	}

	// Set.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s member:%06d", i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "hashtable" {
		t.Fatalf("set encoding = %q want hashtable", enc)
	}
	members := aggScanAll(t, r, c, "SSCAN", "s", " COUNT 16")
	seen := map[string]bool{}
	for _, m := range members {
		seen[m] = true
	}
	if len(seen) != n {
		t.Fatalf("SSCAN covered %d members want %d", len(seen), n)
	}

	// Sorted set: scan must visit only the member-index family and pair each member
	// with its score, never leaking the score-index rows.
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d zmember:%06d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING z"); enc != "skiplist" {
		t.Fatalf("zset encoding = %q want skiplist", enc)
	}
	zflat := aggScanAll(t, r, c, "ZSCAN", "z", " COUNT 16")
	zgot := map[string]string{}
	for i := 0; i+1 < len(zflat); i += 2 {
		zgot[zflat[i]] = zflat[i+1]
	}
	if len(zgot) != n {
		t.Fatalf("ZSCAN covered %d members want %d", len(zgot), n)
	}
	for i := range n {
		m := fmt.Sprintf("zmember:%06d", i)
		if zgot[m] != strconv.Itoa(i) {
			t.Fatalf("ZSCAN member %s score = %q want %d", m, zgot[m], i)
		}
	}
}

// TestAggScanCollMatch checks MATCH still filters correctly across the paged walk,
// and that COUNT bounds work examined rather than results: a MATCH that keeps a few
// of many still terminates and returns exactly the matches.
func TestAggScanCollMatch(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s plain:%06d", i))
	}
	// A handful of needles among the haystack.
	for i := range 5 {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD s needle:%d", i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "hashtable" {
		t.Fatalf("set encoding = %q want hashtable", enc)
	}
	members := aggScanAll(t, r, c, "SSCAN", "s", " MATCH needle:* COUNT 16")
	if len(members) != 5 {
		t.Fatalf("SSCAN MATCH found %d want 5: %v", len(members), members)
	}
}

// TestSScanCollPageIsBounded is the allocation witness that a single coll-form scan
// page does not materialize the whole set. We build a large set with padded members
// so a whole-set clone would move on the order of a megabyte, then assert one SSCAN
// page allocates a small constant rather than scaling with the member count.
func TestSScanCollPageIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
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
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("s"), member(i)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("s")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("set not in coll form: OBJECT ENCODING = %q", got)
	}

	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SSCAN"), []byte("s"), []byte("0"), []byte("COUNT"), []byte("16")})
	})
	// One page reads at most COUNT rows and copies them; a whole-set clone would be
	// on the order of n. Bound it well below n so the materialize path cannot return.
	if allocs > 200 {
		t.Fatalf("one SSCAN page over a %d-member set allocated %.0f objects per run; "+
			"a page should be O(COUNT), not O(n)", n, allocs)
	}
}
