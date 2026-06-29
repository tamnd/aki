package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// sendBulk issues cmd and returns the bulk-string body, reading the length line
// and then the value line. An empty bulk ($-1) returns the empty string.
func sendBulk(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) string {
	t.Helper()
	hdr := sendLine(t, r, c, cmd)
	if hdr == "$-1" {
		return ""
	}
	if !strings.HasPrefix(hdr, "$") {
		t.Fatalf("%s: not a bulk reply: %q", cmd, hdr)
	}
	return sendLine(t, r, c, "")
}

// TestListMoveCollKeepsForm guards LMOVE and RPOPLPUSH against the demote trap on
// coll-form lists. The move routed both ends through getList, cloned each whole
// list, and rewrote both as blobs, which moved promoted keys back to blob form so
// the next reader paid the materialize again. The streamed path pops one boundary
// row from src and pushes one row onto dst, leaving both lists coll.
func TestListMoveCollKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	// Two coll lists of 300 elements each, well past the 128-entry threshold.
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH src s:%03d", i))
		sendLine(t, r, c, fmt.Sprintf("RPUSH dst d:%03d", i))
	}
	if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
		t.Fatal("300-element lists should be stored coll")
	}

	// RPOPLPUSH moves the src tail onto the dst head.
	if got := sendBulk(t, r, c, "RPOPLPUSH src dst"); got != "s:299" {
		t.Fatalf("RPOPLPUSH src dst = %q want s:299", got)
	}
	// LMOVE in every direction.
	sendLine(t, r, c, "LMOVE src dst LEFT RIGHT")
	sendLine(t, r, c, "LMOVE src dst RIGHT LEFT")
	sendLine(t, r, c, "LMOVE src dst LEFT LEFT")
	if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
		t.Fatal("a streamed move must leave both lists coll, not demote them to blob")
	}

	// A same-key rotate must also keep the list coll.
	sendLine(t, r, c, "RPOPLPUSH src src")
	sendLine(t, r, c, "LMOVE dst dst RIGHT LEFT")
	if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
		t.Fatal("a same-key rotate must leave the list coll")
	}
}

// TestListMoveCollIsBounded witnesses that a move touches a bounded number of rows
// rather than cloning whole lists. A same-key rotate pops one tail row and pushes
// one head row on a 2000-element coll list, so the streamed path stays a small
// constant per run while a materialize would clone all 2000 elements.
func TestListMoveCollIsBounded(t *testing.T) {
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

	// A same-key rotate leaves the length unchanged, so it repeats cleanly.
	rotate := [][]byte{[]byte("RPOPLPUSH"), []byte("l"), []byte("l")}
	allocs := testing.AllocsPerRun(20, func() {
		conn.ResetOut()
		d.Handle(conn, rotate)
	})
	if allocs > 250 {
		t.Fatalf("RPOPLPUSH rotate on a %d-element coll list allocated %.0f objects per run; "+
			"the streamed path should touch two rows, not clone n", n, allocs)
	}
}

// TestLMoveCollMatchesNaive checks the streamed coll-form move returns and leaves
// exactly what a materialized move would, across every direction and a same-key
// rotate, with both lists kept large enough to stay coll.
func TestLMoveCollMatchesNaive(t *testing.T) {
	cases := []struct {
		name             string
		cmd              string
		fromLeft, toLeft bool
	}{
		{"rpoplpush", "RPOPLPUSH src dst", false, true},
		{"lmove-LL", "LMOVE src dst LEFT LEFT", true, true},
		{"lmove-LR", "LMOVE src dst LEFT RIGHT", true, false},
		{"lmove-RL", "LMOVE src dst RIGHT LEFT", false, true},
		{"lmove-RR", "LMOVE src dst RIGHT RIGHT", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, eng := startDataEng(t)
			var src, dst []string
			for i := range 300 {
				sendLine(t, r, c, fmt.Sprintf("RPUSH src s:%03d", i))
				sendLine(t, r, c, fmt.Sprintf("RPUSH dst d:%03d", i))
				src = append(src, fmt.Sprintf("s:%03d", i))
				dst = append(dst, fmt.Sprintf("d:%03d", i))
			}
			if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
				t.Fatal("lists should be coll")
			}

			// Apply the move to the naive model.
			var elem string
			if tc.fromLeft {
				elem, src = src[0], src[1:]
			} else {
				elem, src = src[len(src)-1], src[:len(src)-1]
			}
			if tc.toLeft {
				dst = append([]string{elem}, dst...)
			} else {
				dst = append(dst, elem)
			}

			got := sendBulk(t, r, c, tc.cmd)
			if got != elem {
				t.Fatalf("%s returned %q want %q", tc.cmd, got, elem)
			}
			gotSrc := readArray(t, r, c, "LRANGE src 0 -1")
			gotDst := readArray(t, r, c, "LRANGE dst 0 -1")
			if !eqStr(gotSrc, src) {
				t.Fatalf("src after %s mismatch: got %d want %d elems", tc.cmd, len(gotSrc), len(src))
			}
			if !eqStr(gotDst, dst) {
				t.Fatalf("dst after %s mismatch: got %d want %d elems", tc.cmd, len(gotDst), len(dst))
			}
			if !listIsColl(t, eng, "src") || !listIsColl(t, eng, "dst") {
				t.Fatalf("%s demoted a list off coll form", tc.cmd)
			}
		})
	}
}

// TestLMoveCollSameKeyRotate checks a same-key move rotates the list end to end
// and keeps it coll, the case where pop empties then push recreates the key.
func TestLMoveCollSameKeyRotate(t *testing.T) {
	r, c, eng := startDataEng(t)
	var l []string
	for i := range 300 {
		sendLine(t, r, c, fmt.Sprintf("RPUSH l e:%03d", i))
		l = append(l, fmt.Sprintf("e:%03d", i))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list should be coll")
	}
	// RPOPLPUSH l l moves the tail to the head.
	elem := l[len(l)-1]
	l = append([]string{elem}, l[:len(l)-1]...)
	if got := sendBulk(t, r, c, "RPOPLPUSH l l"); got != elem {
		t.Fatalf("RPOPLPUSH l l = %q want %q", got, elem)
	}
	gotL := readArray(t, r, c, "LRANGE l 0 -1")
	if !eqStr(gotL, l) {
		t.Fatalf("rotate mismatch: got %d want %d", len(gotL), len(l))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("rotate must keep the list coll")
	}
}

// eqStr compares the LRANGE reply (bulk bodies, prefixes stripped) against the
// model slice.
func eqStr(got, want []string) bool {
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
