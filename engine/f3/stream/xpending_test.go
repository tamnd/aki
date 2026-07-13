package stream

import (
	"strconv"
	"testing"
)

// The XPENDING suite (spec 2064/f3/14 section 7.7). The summary form reads the
// counts and the tree end-peeks; the extended form seeks and walks the PEL tree,
// filtering by ID window, idle floor, and optional consumer. Both are pure ledger
// reads.

// pendingRows decodes an extended-XPENDING reply into [id, consumer, idle, count]
// rows.
func pendingRows(t *testing.T, raw []byte) [][4]string {
	t.Helper()
	arr := decodeReply(t, raw).([]any)
	out := make([][4]string, len(arr))
	for i, row := range arr {
		r := row.([]any)
		out[i] = [4]string{r[0].(string), r[1].(string), r[2].(string), r[3].(string)}
	}
	return out
}

func TestXpendingSummaryEmpty(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	got := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	if got[0].(string) != "0" || got[1] != nil || got[2] != nil || got[3] != nil {
		t.Fatalf("empty summary = %v, want [0 nil nil nil]", render(got))
	}
}

func TestXpendingSummaryWithEntries(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	got := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	if got[0].(string) != "3" || got[1].(string) != "1-0" || got[2].(string) != "3-0" {
		t.Fatalf("summary head = %v, want [3 1-0 3-0 ...]", render(got))
	}
	consumers := got[3].([]any)
	if len(consumers) != 1 {
		t.Fatalf("consumers = %v, want one", consumers)
	}
	row := consumers[0].([]any)
	if row[0].(string) != "c" || row[1].(string) != "3" {
		t.Fatalf("consumer row = %v, want [c 3]", row)
	}
}

func TestXpendingSummaryTwoConsumersSorted(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "beta", "COUNT", "1", "STREAMS", "s", ">")
	do(t, c, opXreadgroup, "GROUP", "g", "alpha", "STREAMS", "s", ">")
	got := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	consumers := got[3].([]any)
	if len(consumers) != 2 {
		t.Fatalf("consumers = %v, want two", consumers)
	}
	// Name order, alpha before beta, regardless of read order.
	if consumers[0].([]any)[0].(string) != "alpha" || consumers[1].([]any)[0].(string) != "beta" {
		t.Fatalf("consumer order = %v, want alpha then beta", consumers)
	}
}

func TestXpendingExtendedRows(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 3 {
		t.Fatalf("rows = %v, want 3", rows)
	}
	if rows[0][0] != "1-0" || rows[0][1] != "c" || rows[0][3] != "1" {
		t.Fatalf("row 0 = %v, want [1-0 c _ 1]", rows[0])
	}
	if idle, err := strconv.Atoi(rows[0][2]); err != nil || idle < 0 {
		t.Fatalf("row 0 idle = %q, want a non-negative integer", rows[0][2])
	}
}

func TestXpendingExtendedCountCap(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 5)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "2"))
	if len(rows) != 2 || rows[0][0] != "1-0" || rows[1][0] != "2-0" {
		t.Fatalf("capped rows = %v, want 1-0 and 2-0", rows)
	}
}

func TestXpendingExtendedConsumerFilter(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "COUNT", "2", "STREAMS", "s", ">")
	do(t, c, opXreadgroup, "GROUP", "g", "c2", "STREAMS", "s", ">")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10", "c2"))
	if len(rows) != 1 || rows[0][0] != "3-0" || rows[0][1] != "c2" {
		t.Fatalf("c2 rows = %v, want just 3-0", rows)
	}
	// An unknown consumer owns nothing.
	if rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10", "ghost")); len(rows) != 0 {
		t.Fatalf("ghost rows = %v, want empty", rows)
	}
}

func TestXpendingExtendedIdleFilter(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	// The entries were just delivered, so a 100s idle floor excludes them all.
	if rows := pendingRows(t, do(t, c, opXpending, "s", "g", "IDLE", "100000", "-", "+", "10")); len(rows) != 0 {
		t.Fatalf("idle-filtered rows = %v, want empty", rows)
	}
	// A zero floor keeps them.
	if rows := pendingRows(t, do(t, c, opXpending, "s", "g", "IDLE", "0", "-", "+", "10")); len(rows) != 2 {
		t.Fatalf("idle-0 rows = %v, want 2", rows)
	}
}

func TestXpendingExtendedZeroCount(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	if rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "0")); len(rows) != 0 {
		t.Fatalf("zero-count rows = %v, want empty", rows)
	}
}

func TestXpendingExtendedWindow(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 4)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "2-0", "3-0", "10"))
	if len(rows) != 2 || rows[0][0] != "2-0" || rows[1][0] != "3-0" {
		t.Fatalf("windowed rows = %v, want 2-0 and 3-0", rows)
	}
}

func TestXpendingNogroup(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXpending, "nokey", "g"), "NOGROUP No such key 'nokey' or consumer group 'g'")
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXpending, "s", "nope"), "NOGROUP No such key 's' or consumer group 'nope'")
}

func TestXpendingWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "str", "v")
	wantErr(t, do(t, c, opXpending, "str", "g"), wrongType)
}

func TestXpendingSyntax(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXpending, "s", "g", "-", "+"), "ERR syntax error")
	wantErr(t, do(t, c, opXpending, "s", "g", "-", "+", "notanint"), "ERR value is not an integer or out of range")
	wantErr(t, do(t, c, opXpending, "s", "g", "bad", "+", "10"), errInvalidID)
	wantErr(t, do(t, c, opXpending, "s", "g", "IDLE", "notanint", "-", "+", "10"), "ERR value is not an integer or out of range")
	wantErr(t, do(t, c, opXpending, "s", "g", "IDLE"), "ERR syntax error")
	wantErr(t, do(t, c, opXpending, "s", "g", "-", "+", "10", "consumer", "extra"), "ERR syntax error")
}
