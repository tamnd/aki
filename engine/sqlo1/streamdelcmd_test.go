package sqlo1

import (
	"strconv"
	"strings"
	"testing"
)

// TestXdelWire pins XDEL against Redis 8.8: the key check precedes the
// ID parse so a missing key answers 0 even with a bad ID, one bad ID
// aborts the whole call with nothing deleted, duplicates count once,
// the max-deleted-entry-id root field advances to the largest ID that
// actually fell, and a stream deleted to empty keeps its key and its
// last generated ID. The PEL keeps deleted entries until a claim drops
// them, and a destroyed group's pending state is gone on recreate.
func TestXdelWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("SET", "str", "x")
	for _, id := range []string{"1-1", "2-2", "3-3"} {
		do("XADD", "s", id, "f", "v")
	}

	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XDEL"}, "-ERR wrong number of arguments for 'xdel' command\r\n"},
		{[]string{"XDEL", "s"}, "-ERR wrong number of arguments for 'xdel' command\r\n"},
		// The key check runs before any ID parses.
		{[]string{"XDEL", "nosuchkey", "notanid"}, ":0\r\n"},
		{[]string{"XDEL", "str", "notanid"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XDEL", "str", "1-1"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XDEL", "s", "notanid"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XDEL", "s", "1-1", "notanid"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XDEL", "s", "-"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XDEL", "s", "5-*"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
	}
	for _, e := range errs {
		if got := do(e.args...); got != e.want {
			t.Fatalf("%v = %q, want %q", e.args, got, e.want)
		}
	}
	// The aborted call deleted nothing.
	if got := do("XLEN", "s"); got != ":3\r\n" {
		t.Fatalf("XLEN after aborts = %q", got)
	}

	// A duplicate ID counts once and a never-existing ID counts zero.
	if got := do("XDEL", "s", "1-1", "1-1", "9-9"); got != ":1\r\n" {
		t.Fatalf("XDEL dup = %q", got)
	}
	if got := do("XLEN", "s"); got != ":2\r\n" {
		t.Fatalf("XLEN = %q", got)
	}
	if got := do("XRANGE", "s", "-", "+"); got != xarr(xent("2-2", "f", "v"), xent("3-3", "f", "v")) {
		t.Fatalf("XRANGE = %q", got)
	}

	// max-deleted-entry-id holds the largest deleted ID: deleting 2-2
	// after 3-3 does not move it back, and a miss does not move it.
	maxDel := func() string {
		t.Helper()
		rep := do("XINFO", "STREAM", "s")
		_, rest, ok := strings.Cut(rep, "max-deleted-entry-id\r\n")
		if !ok {
			t.Fatalf("XINFO STREAM has no max-deleted-entry-id: %q", rep)
		}
		parts := strings.SplitN(rest, "\r\n", 3)
		return parts[1]
	}
	if got := maxDel(); got != "1-1" {
		t.Fatalf("maxDel = %q, want 1-1", got)
	}
	if got := do("XDEL", "s", "3-3"); got != ":1\r\n" {
		t.Fatalf("XDEL 3-3 = %q", got)
	}
	if got := maxDel(); got != "3-3" {
		t.Fatalf("maxDel = %q, want 3-3", got)
	}
	if got := do("XDEL", "s", "2-2"); got != ":1\r\n" {
		t.Fatalf("XDEL 2-2 = %q", got)
	}
	if got := do("XDEL", "s", "99-9"); got != ":0\r\n" {
		t.Fatalf("XDEL miss = %q", got)
	}
	if got := maxDel(); got != "3-3" {
		t.Fatalf("maxDel = %q, want 3-3 still", got)
	}

	// Deleted to empty, the key and the last generated ID survive.
	if got := do("XLEN", "s"); got != ":0\r\n" {
		t.Fatalf("XLEN = %q", got)
	}
	if got := do("TYPE", "s"); got != "+stream\r\n" {
		t.Fatalf("TYPE = %q", got)
	}
	if got := do("XADD", "s", "3-3", "f", "v"); got != "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n" {
		t.Fatalf("XADD 3-3 = %q", got)
	}
	if got := do("XADD", "s", "3-4", "f", "v"); got != "$3\r\n3-4\r\n" {
		t.Fatalf("XADD 3-4 = %q", got)
	}

	// A bare ms reads as ms-0, so it misses 5-1.
	do("XADD", "s", "5-1", "f", "v")
	if got := do("XDEL", "s", "5"); got != ":0\r\n" {
		t.Fatalf("XDEL bare ms = %q", got)
	}

	// The PEL keeps a deleted entry: the summary still counts it and
	// the history read renders its nil row.
	do("XADD", "bs", "1-1", "f", "v")
	do("XGROUP", "CREATE", "bs", "g", "0")
	do("XREADGROUP", "GROUP", "g", "c", "COUNT", "10", "STREAMS", "bs", ">")
	if got := do("XDEL", "bs", "1-1"); got != ":1\r\n" {
		t.Fatalf("XDEL pending = %q", got)
	}
	wantPend := "*4\r\n:1\r\n$3\r\n1-1\r\n$3\r\n1-1\r\n*1\r\n*2\r\n$1\r\nc\r\n$1\r\n1\r\n"
	if got := do("XPENDING", "bs", "g"); got != wantPend {
		t.Fatalf("XPENDING = %q, want %q", got, wantPend)
	}
	wantHist := "*1\r\n*2\r\n$2\r\nbs\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*-1\r\n"
	if got := do("XREADGROUP", "GROUP", "g", "c", "STREAMS", "bs", "0"); got != wantHist {
		t.Fatalf("history = %q, want %q", got, wantHist)
	}

	// Destroy sweeps the PEL; the recreated group starts pending-free.
	if got := do("XGROUP", "DESTROY", "bs", "g"); got != ":1\r\n" {
		t.Fatalf("DESTROY = %q", got)
	}
	do("XGROUP", "CREATE", "bs", "g", "0")
	if got := do("XPENDING", "bs", "g"); got != "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n" {
		t.Fatalf("XPENDING after recreate = %q", got)
	}
}

// TestXdelPaged drives whole-run and whole-page death through the
// dialed caps (three flat runs, two per page, four index slots, with
// kilobyte values putting two entries in a run): an interior run
// falls whole, a page emptied by deletes drops from the index, one
// call crosses pages, and a stream deleted to empty keeps appending.
func TestXdelPaged(t *testing.T) {
	defer SetStreamFenceCapsForTest(3, 2, 4)()
	do, _ := dispatchServer(t)
	med := strings.Repeat("m", 1800)
	alive := map[int]bool{}
	for ms := 1; ms <= 16; ms++ {
		do("XADD", "s", strconv.Itoa(ms)+"-1", "v", med)
		alive[ms] = true
	}
	check := func() {
		t.Helper()
		n := 0
		var want strings.Builder
		for ms := 1; ms <= 20; ms++ {
			if alive[ms] {
				n++
				want.WriteString(xent(strconv.Itoa(ms)+"-1", "v", med))
			}
		}
		if got := do("XLEN", "s"); got != ":"+strconv.Itoa(n)+"\r\n" {
			t.Fatalf("XLEN = %q, want %d", got, n)
		}
		if got := do("XRANGE", "s", "-", "+"); got != "*"+strconv.Itoa(n)+"\r\n"+want.String() {
			t.Fatalf("XRANGE mismatch at %d live entries", n)
		}
	}
	xdel := func(want int, ids ...string) {
		t.Helper()
		args := append([]string{"XDEL", "s"}, ids...)
		if got := do(args...); got != ":"+strconv.Itoa(want)+"\r\n" {
			t.Fatalf("XDEL %v = %q, want %d", ids, got, want)
		}
		for _, id := range ids {
			ms, _, _ := strings.Cut(id, "-")
			n, _ := strconv.Atoi(ms)
			delete(alive, n)
		}
		check()
	}

	// One entry of an interior run, then its partner kills the run.
	xdel(1, "5-1")
	xdel(1, "6-1")
	// The rest of the first page: runs (1,2) and (3,4) fall, the page
	// dies, and a range that starts inside the gap answers empty.
	xdel(4, "1-1", "2-2", "2-1", "3-1", "4-1")
	if got := do("XRANGE", "s", "1", "6"); got != "*0\r\n" {
		t.Fatalf("XRANGE gap = %q", got)
	}
	// One call crossing pages.
	xdel(3, "7-1", "9-1", "10-1")
	// Everything else, to empty, and generation resumes above.
	xdel(7, "8-1", "11-1", "12-1", "13-1", "14-1", "15-1", "16-1")
	if got := do("TYPE", "s"); got != "+stream\r\n" {
		t.Fatalf("TYPE = %q", got)
	}
	do("XADD", "s", "20-1", "v", med)
	alive[20] = true
	check()
}
