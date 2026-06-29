package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
)

// zsetIsColl reports whether key is actually stored in coll form (a score-index
// sub-tree), reading the header directly rather than trusting OBJECT ENCODING,
// which reports skiplist for a large set whether it is a sub-tree or a blob and so
// would hide a demote.
func zsetIsColl(t *testing.T, eng *Engine, key string) bool {
	t.Helper()
	var coll, found bool
	if err := eng.view(0, func(db *keyspace.DB) error {
		hdr, ok, err := zsetHeader(db, []byte(key))
		if err != nil {
			return err
		}
		found = ok
		coll = ok && hdr.IsColl()
		return nil
	}); err != nil {
		t.Fatalf("view %q: %v", key, err)
	}
	if !found {
		t.Fatalf("key %q absent", key)
	}
	return coll
}

// readZMPop reads the ZMPOP/BZMPOP reply shape: an outer two-element array of the
// key and a nested array of [member, score] pairs. It returns the key and the
// flattened member/score pairs.
func readZMPop(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) (string, [][2]string) {
	t.Helper()
	head := sendLine(t, r, c, cmd)
	if head != "*2" {
		t.Fatalf("%s: expected *2, got %q", cmd, head)
	}
	key := readElem(t, r)
	innerLine, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("%s: read inner header: %v", cmd, err)
	}
	inner := strings.TrimRight(innerLine, "\r\n")
	if len(inner) == 0 || inner[0] != '*' {
		t.Fatalf("%s: expected inner array, got %q", cmd, inner)
	}
	n, err := strconv.Atoi(inner[1:])
	if err != nil {
		t.Fatalf("%s: bad inner array len %q", cmd, inner)
	}
	out := make([][2]string, 0, n)
	for range n {
		pairLine, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("%s: read pair header: %v", cmd, err)
		}
		if p := strings.TrimRight(pairLine, "\r\n"); p != "*2" {
			t.Fatalf("%s: expected pair *2, got %q", cmd, p)
		}
		member := readElem(t, r)
		score := readElem(t, r)
		out = append(out, [2]string{member, score})
	}
	return key, out
}

// The blocking sorted-set pop family (BZPOPMIN, BZPOPMAX, BZMPOP) and the
// non-blocking ZMPOP cloned the whole set through getZSet on every served attempt
// and wrote the leftover back as a blob, materializing and demoting a large coll
// set per attempt. They now pop the boundary rows in place through zsetPopN, so a
// served attempt touches the end window and the set stays coll.

// TestBlockingZPopCollKeepsForm checks BZPOPMIN and BZPOPMAX with data present pop
// one boundary member and leave the set coll.
func TestBlockingZPopCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := range 500 {
		sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("500-member zset should be coll")
	}
	// BZPOPMIN with data present returns the lowest member immediately.
	got := readArray(t, r, c, "BZPOPMIN z 0")
	if len(got) != 3 || got[0] != "z" || got[1] != "m:000000" || got[2] != "0" {
		t.Fatalf("BZPOPMIN z 0 = %v want [z m:000000 0]", got)
	}
	// BZPOPMAX with data present returns the highest member immediately.
	got = readArray(t, r, c, "BZPOPMAX z 0")
	if len(got) != 3 || got[0] != "z" || got[1] != "m:000499" || got[2] != "499" {
		t.Fatalf("BZPOPMAX z 0 = %v want [z m:000499 499]", got)
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("a blocking pop with data present must leave the set coll")
	}
}

// TestBzmpopCollKeepsForm checks BZMPOP and the non-blocking ZMPOP with data
// present pop a bounded count and leave the set coll.
func TestBzmpopCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := range 500 {
		sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("zset should be coll")
	}
	// ZMPOP (non-blocking) pops the lowest three.
	key, pairs := readZMPop(t, r, c, "ZMPOP 1 z MIN COUNT 3")
	if key != "z" || len(pairs) != 3 {
		t.Fatalf("ZMPOP = %q %v", key, pairs)
	}
	// BZMPOP pops the highest two.
	key, pairs = readZMPop(t, r, c, "BZMPOP 0 1 z MAX COUNT 2")
	if key != "z" || len(pairs) != 2 {
		t.Fatalf("BZMPOP = %q %v", key, pairs)
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("a count pop must leave the set coll")
	}
}

// TestBzmpopCollMatchesNaive checks the popped members and order match a naive
// model for both directions across the non-blocking and blocking forms.
func TestBzmpopCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	const n = 500
	for i := range n {
		sendLine(t, r, c, fmt.Sprintf("ZADD z %d m:%06d", i, i))
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("zset should be coll")
	}

	// ZMPOP MIN COUNT 3 pops the three lowest scores, ascending.
	_, pairs := readZMPop(t, r, c, "ZMPOP 1 z MIN COUNT 3")
	want := [][2]string{{"m:000000", "0"}, {"m:000001", "1"}, {"m:000002", "2"}}
	if !eqPairs(pairs, want) {
		t.Fatalf("ZMPOP MIN COUNT 3 = %v want %v", pairs, want)
	}

	// BZMPOP MAX COUNT 2 pops the two highest scores, descending.
	_, pairs = readZMPop(t, r, c, "BZMPOP 0 1 z MAX COUNT 2")
	want = [][2]string{
		{fmt.Sprintf("m:%06d", n-1), strconv.Itoa(n - 1)},
		{fmt.Sprintf("m:%06d", n-2), strconv.Itoa(n - 2)},
	}
	if !eqPairs(pairs, want) {
		t.Fatalf("BZMPOP MAX COUNT 2 = %v want %v", pairs, want)
	}

	// The card dropped by exactly five and the set is still coll.
	if card := sendLine(t, r, c, "ZCARD z"); card != fmt.Sprintf(":%d", n-5) {
		t.Fatalf("ZCARD after pops = %q want :%d", card, n-5)
	}
	if !zsetIsColl(t, eng, "z") {
		t.Fatal("set must stay coll")
	}
}

// TestBlockingZPopCollIsBounded witnesses that a BZPOPMIN served by present data
// pops one row rather than cloning the set. The popped member is re-added inside
// the witness so the pop has data every run and the size returns to where it
// started. Padded members make a whole-set clone move about a megabyte.
func TestBlockingZPopCollIsBounded(t *testing.T) {
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
	add := func(i int) {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), []byte("z"), []byte(strconv.Itoa(i)), member(i)})
	}
	for i := range n {
		add(i)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("z")})
	if got := string(conn.OutBytes()); got != "$8\r\nskiplist\r\n" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	// Each run pops the lowest member with BZPOPMIN (data present, served at once)
	// then re-adds it, so the size is stable and the served pop always has data.
	pop := [][]byte{[]byte("BZPOPMIN"), []byte("z"), []byte("0")}
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, pop)
		add(0)
	})
	if allocs > 400 {
		t.Fatalf("BZPOPMIN served from a %d-member coll set allocated %.0f objects per run; "+
			"the served path should pop one row, not clone n", n, allocs)
	}
}

// eqPairs reports whether two member/score pair slices are equal.
func eqPairs(got, want [][2]string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
