package f1srv

import (
	"bufio"
	"testing"
)

// Slice 3 (impl/34) stops writing an f1raw row for a position that a lock-free push lands in the
// resident ring: the ring is the only store, pop reads it with no hash probe, and drainEvict flushes
// the survivors to rows when the window retires. These tests drive a list past window admission so
// later pushes are ring-resident, then exercise every path that reads those positions back: pop
// straight off the ring, and the readers that must retire the window first (LRANGE, LINDEX, DUMP,
// COPY, RENAME, SORT). They assert observable order and values, which is what the row/ring split has
// to preserve.
//
// Admission is repeat-traffic gated (list.go cmdPush): the first push to a fresh key creates the
// header with no window, the second push admits a window seeded from the header (its positions stay
// f1raw rows, the pre-block), and every push after that appends lock-free into the ring. So two
// warmup pushes then a third push give a list whose tail positions are ring-resident.

// primeResidentList pushes so key holds [a b c d e f g h] with positions 0..3 as pre-block f1raw
// rows and positions 4..7 ring-resident, then returns. It leaves nothing to read on the wire.
func primeResidentList(t *testing.T, rw *bufio.ReadWriter, key string) {
	t.Helper()
	cmd(t, rw, "RPUSH", key, "a", "b")
	expect(t, rw, ":2") // fresh key, no window
	cmd(t, rw, "RPUSH", key, "c", "d")
	expect(t, rw, ":4") // repeat traffic, admits window seeded [0,4)
	cmd(t, rw, "RPUSH", key, "e", "f", "g", "h")
	expect(t, rw, ":8") // lock-free, positions 4..7 land in the ring only
}

// TestListResidentPopFromRing pops the ring-resident tail and the pre-block head without any reader
// retiring the window first, so the pops must come straight off the ring (tail) and off the f1raw
// rows (head) and still return the right values in the right order.
func TestListResidentPopFromRing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	primeResidentList(t, rw, "l")

	// RPOP two: positions 7 then 6, both ring-resident, returned tail-first.
	if got := lrangeCall(t, rw, "RPOP", "l", "2"); !eqStrs(got, []string{"h", "g"}) {
		t.Fatalf("RPOP l 2 = %v, want [h g]", got)
	}
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":6")

	// LPOP three: positions 0,1,2, all pre-block f1raw rows, returned head-first.
	if got := lrangeCall(t, rw, "LPOP", "l", "3"); !eqStrs(got, []string{"a", "b", "c"}) {
		t.Fatalf("LPOP l 3 = %v, want [a b c]", got)
	}
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":3")

	// What is left spans the boundary: pos 3 (row) and pos 4,5 (ring). LRANGE retires the window,
	// flushing the ring survivors to rows, and must read the whole span in order.
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"d", "e", "f"}) {
		t.Fatalf("LRANGE after boundary pops = %v, want [d e f]", got)
	}
}

// TestListResidentReadersRetire checks every reader that has to drainEvict the window before it can
// read positional rows: each must see the ring-resident tail as if it were already flushed.
func TestListResidentReadersRetire(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// LINDEX into a ring-resident position.
	primeResidentList(t, rw, "li")
	cmd(t, rw, "LINDEX", "li", "6")
	expect(t, rw, "$g")
	cmd(t, rw, "LINDEX", "li", "-1")
	expect(t, rw, "$h")

	// LRANGE across the whole list, spanning pre-block and ring.
	primeResidentList(t, rw, "lr")
	if got := lrangeCall(t, rw, "LRANGE", "lr", "0", "-1"); !eqStrs(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("LRANGE full = %v", got)
	}

	// LPOS finds a ring-resident element.
	primeResidentList(t, rw, "lp")
	cmd(t, rw, "LPOS", "lp", "g")
	expect(t, rw, ":6")

	// COPY duplicates every row, so the destination must carry the ring survivors too.
	primeResidentList(t, rw, "lc")
	cmd(t, rw, "COPY", "lc", "lc2")
	expect(t, rw, ":1")
	if got := lrangeCall(t, rw, "LRANGE", "lc2", "0", "-1"); !eqStrs(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("COPY dst = %v", got)
	}

	// RENAME moves every row, positions preserved.
	primeResidentList(t, rw, "ln")
	cmd(t, rw, "RENAME", "ln", "ln2")
	expect(t, rw, "+OK")
	if got := lrangeCall(t, rw, "LRANGE", "ln2", "0", "-1"); !eqStrs(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("RENAME dst = %v", got)
	}

	// SORT ALPHA reads the whole list off the header window, so it must retire and flush first.
	primeResidentList(t, rw, "ls")
	if got := lrangeCall(t, rw, "SORT", "ls", "ALPHA"); !eqStrs(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("SORT = %v", got)
	}
}

// TestListResidentDumpRestore round-trips a list with ring-resident positions through DUMP/RESTORE:
// DUMP must retire the window and serialize the flushed rows, and the restored copy must match.
func TestListResidentDumpRestore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	primeResidentList(t, rw, "ld")
	cmd(t, rw, "DUMP", "ld")
	blob := readReply(t, rw)
	if len(blob) == 0 || blob[0] != '$' {
		t.Fatalf("DUMP = %q, want a bulk payload", blob)
	}
	payload := blob[1:]

	cmd(t, rw, "RESTORE", "ld2", "0", payload)
	expect(t, rw, "+OK")
	if got := lrangeCall(t, rw, "LRANGE", "ld2", "0", "-1"); !eqStrs(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("RESTORE copy = %v", got)
	}
}
