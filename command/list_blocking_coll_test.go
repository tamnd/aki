package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// readLMPop reads the LMPOP/BLMPOP reply shape: an outer two-element array of the
// key and a nested array of the popped elements. It returns the key and elements.
func readLMPop(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) (string, []string) {
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
	out := make([]string, 0, n)
	for range n {
		out = append(out, readElem(t, r))
	}
	return key, out
}

// The blocking pop and move family (BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP) and
// the non-blocking LMPOP all served a request by cloning the whole list through
// getList and writing it back as a blob, so a large coll list was materialized and
// demoted on every served attempt. They now route to the same bounded point pop
// and push the non-blocking pops use, so a served request touches only the
// boundary rows and the list stays coll.

// TestBlockingPopCollKeepsForm checks BLPOP and BRPOP with data present pop one
// boundary element and leave the list coll.
func TestBlockingPopCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH l e:%03d", i))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("300-element list should be coll")
	}
	// BLPOP with data present returns the head immediately.
	got := readArray(t, r, c, "BLPOP l 0")
	if len(got) != 2 || got[0] != "l" || got[1] != "e:000" {
		t.Fatalf("BLPOP l 0 = %v want [l e:000]", got)
	}
	// BRPOP with data present returns the tail immediately.
	got = readArray(t, r, c, "BRPOP l 0")
	if len(got) != 2 || got[0] != "l" || got[1] != "e:299" {
		t.Fatalf("BRPOP l 0 = %v want [l e:299]", got)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("a blocking pop with data present must leave the list coll")
	}
}

// TestBlockingMoveCollKeepsForm checks BRPOPLPUSH and BLMOVE with data present move
// one element and leave both lists coll.
func TestBlockingMoveCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH src s:%03d", i))
		sendLine(t, r, c, fmt.Sprintf("RPUSH dst d:%03d", i))
	}
	if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
		t.Fatal("lists should be coll")
	}
	if got := sendBulk(t, r, c, "BRPOPLPUSH src dst 0"); got != "s:299" {
		t.Fatalf("BRPOPLPUSH src dst 0 = %q want s:299", got)
	}
	if got := sendBulk(t, r, c, "BLMOVE src dst LEFT RIGHT 0"); got != "s:000" {
		t.Fatalf("BLMOVE src dst LEFT RIGHT 0 = %q want s:000", got)
	}
	if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
		t.Fatal("a blocking move with data present must leave both lists coll")
	}
}

// TestBlmpopCollKeepsForm checks BLMPOP and the non-blocking LMPOP with data
// present pop a bounded count and leave the list coll.
func TestBlmpopCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH l e:%03d", i))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list should be coll")
	}
	// LMPOP (non-blocking) pops the head three.
	key, body := readLMPop(t, r, c, "LMPOP 1 l LEFT COUNT 3")
	if key != "l" || len(body) != 3 {
		t.Fatalf("LMPOP = %q %v", key, body)
	}
	// BLMPOP pops the tail two, tail first.
	key, body = readLMPop(t, r, c, "BLMPOP 0 1 l RIGHT COUNT 2")
	if key != "l" || len(body) != 2 {
		t.Fatalf("BLMPOP = %q %v", key, body)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("a count pop must leave the list coll")
	}
}

// TestBlmpopCollMatchesNaive checks the popped elements and order match a naive
// model for both directions, across the non-blocking and blocking forms.
func TestBlmpopCollMatchesNaive(t *testing.T) {
	r, c, eng := startDataEng(t)
	var l []string
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH l e:%03d", i))
		l = append(l, fmt.Sprintf("e:%03d", i))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list should be coll")
	}

	// LMPOP LEFT COUNT 3 pops the first three head to tail.
	_, body := readLMPop(t, r, c, "LMPOP 1 l LEFT COUNT 3")
	want := l[:3]
	l = l[3:]
	if !eqStr(body, want) {
		t.Fatalf("LMPOP LEFT COUNT 3 = %v want %v", body, want)
	}

	// BLMPOP RIGHT COUNT 2 pops the last two tail first.
	_, body = readLMPop(t, r, c, "BLMPOP 0 1 l RIGHT COUNT 2")
	want = []string{l[len(l)-1], l[len(l)-2]}
	l = l[:len(l)-2]
	if !eqStr(body, want) {
		t.Fatalf("BLMPOP RIGHT COUNT 2 = %v want %v", body, want)
	}

	// The remaining list matches the model.
	rest := readArray(t, r, c, "LRANGE l 0 -1")
	if !eqStr(rest, l) {
		t.Fatalf("remaining list mismatch: got %d want %d", len(rest), len(l))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list must stay coll")
	}
}

// TestBlockingPopCollIsBounded witnesses that a BLPOP served by present data pops
// one row rather than cloning the list. The list is refilled inside the witness so
// the pop has data every run and the length returns to where it started.
func TestBlockingPopCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 2000
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("RPUSH"), []byte("l"), []byte(fmt.Sprintf("e:%05d", i))})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("l")})
	if got := string(conn.OutBytes()); got != "$9\r\nquicklist\r\n" {
		t.Fatalf("list not in coll form: OBJECT ENCODING = %q", got)
	}

	// Each run pops the head with BLPOP (data present, returns immediately) then
	// pushes it back, so the length is stable and the pop always has data.
	pop := [][]byte{[]byte("BLPOP"), []byte("l"), []byte("0")}
	push := [][]byte{[]byte("LPUSH"), []byte("l"), []byte("e:00000")}
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, pop)
		conn.ResetOut()
		d.Handle(conn, push)
	})
	if allocs > 400 {
		t.Fatalf("BLPOP served from a %d-element coll list allocated %.0f objects per run; "+
			"the served path should pop one row, not clone n", n, allocs)
	}
}
