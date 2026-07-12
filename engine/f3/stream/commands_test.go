package stream

import (
	"testing"
)

// The command-level suite: the wire behavior of the stream write path this slice
// ships (spec 2064/f3/14 section 6), driven through the runtime harness. Reply
// shapes, WRONGTYPE, arity, ID validation, and the OBJECT ENCODING answer are all
// checked here; the live differential replays the same shapes against a real
// Redis in a later slice.

func TestXaddExplicitIDs(t *testing.T) {
	c := newHarness(t).NewConn()
	wantBulk(t, do(t, c, opXadd, "s", "1-1", "f", "v"), "1-1")
	wantBulk(t, do(t, c, opXadd, "s", "1-2", "f", "v"), "1-2")
	wantBulk(t, do(t, c, opXadd, "s", "2-0", "f", "v"), "2-0")
	wantInt(t, do(t, c, opXlen, "s"), 3)
}

func TestXaddPartialID(t *testing.T) {
	c := newHarness(t).NewConn()
	// A fixed ms with an auto seq: first is ms-0, then ms-1, and a bare "5" is
	// the same as "5-*".
	wantBulk(t, do(t, c, opXadd, "s", "5-*", "f", "v"), "5-0")
	wantBulk(t, do(t, c, opXadd, "s", "5-*", "f", "v"), "5-1")
	wantBulk(t, do(t, c, opXadd, "s", "5", "f", "v"), "5-2")
	wantBulk(t, do(t, c, opXadd, "s", "6", "f", "v"), "6-0")
}

func TestXaddAutoIDIncreases(t *testing.T) {
	c := newHarness(t).NewConn()
	// The auto clock is the real shard clock, so the exact ms is not pinned, but
	// successive IDs must strictly increase.
	first := bulkReply(t, do(t, c, opXadd, "s", "*", "f", "v"))
	second := bulkReply(t, do(t, c, opXadd, "s", "*", "f", "v"))
	if !idLess(t, first, second) {
		t.Fatalf("auto IDs not increasing: %q then %q", first, second)
	}
	wantInt(t, do(t, c, opXlen, "s"), 2)
}

func TestXaddRejectsNonIncreasing(t *testing.T) {
	c := newHarness(t).NewConn()
	wantBulk(t, do(t, c, opXadd, "s", "5-5", "f", "v"), "5-5")
	// Equal or smaller than the top item is refused, and the stream is unchanged.
	wantErr(t, do(t, c, opXadd, "s", "5-5", "f", "v"),
		"ERR The ID specified in XADD is equal or smaller than the target stream top item")
	wantErr(t, do(t, c, opXadd, "s", "5-4", "f", "v"),
		"ERR The ID specified in XADD is equal or smaller than the target stream top item")
	wantInt(t, do(t, c, opXlen, "s"), 1)
}

func TestXaddRejectsZeroID(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXadd, "s", "0-0", "f", "v"),
		"ERR The ID specified in XADD must be greater than 0-0")
	// The rejected XADD created no key.
	wantInt(t, do(t, c, opXlen, "s"), 0)
}

func TestXaddInvalidID(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXadd, "s", "not-an-id", "f", "v"),
		"ERR Invalid stream ID specified as stream command argument")
}

func TestXaddArity(t *testing.T) {
	c := newHarness(t).NewConn()
	// An odd field-value tail is an arity error.
	wantErr(t, do(t, c, opXadd, "s", "1-1", "f"),
		"ERR wrong number of arguments for 'xadd' command")
}

func TestXaddNomkstream(t *testing.T) {
	c := newHarness(t).NewConn()
	// NOMKSTREAM on a missing key replies nil and creates nothing.
	wantNil(t, do(t, c, opXadd, "s", "NOMKSTREAM", "1-1", "f", "v"))
	wantInt(t, do(t, c, opXlen, "s"), 0)
	// Once the stream exists, NOMKSTREAM appends normally.
	wantBulk(t, do(t, c, opXadd, "s", "1-1", "f", "v"), "1-1")
	wantBulk(t, do(t, c, opXadd, "s", "NOMKSTREAM", "1-2", "f", "v"), "1-2")
	wantInt(t, do(t, c, opXlen, "s"), 2)
}

func TestXaddTrimUnsupported(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXadd, "s", "MAXLEN", "100", "*", "f", "v"),
		"ERR stream trimming is not supported yet")
}

func TestXaddWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	wantStatus(t, do(t, c, opSet, "k", "v"), "OK")
	wantErr(t, do(t, c, opXadd, "k", "1-1", "f", "v"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	wantErr(t, do(t, c, opXlen, "k"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	wantErr(t, do(t, c, opXdel, "k", "1-1"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}

func TestXlenMissing(t *testing.T) {
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opXlen, "nokey"), 0)
}

func TestXdel(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-1", "1-2", "1-3", "2-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	wantInt(t, do(t, c, opXlen, "s"), 4)
	// Delete two present and one absent: count is the present ones only.
	wantInt(t, do(t, c, opXdel, "s", "1-2", "2-0", "9-9"), 2)
	wantInt(t, do(t, c, opXlen, "s"), 2)
	// A second delete of an already-tombstoned ID removes nothing.
	wantInt(t, do(t, c, opXdel, "s", "1-2"), 0)
	wantInt(t, do(t, c, opXlen, "s"), 2)
}

func TestXdelKeepsStreamAndLastID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	wantInt(t, do(t, c, opXdel, "s", "1-1"), 1)
	// The emptied stream is kept, and lastID does not move back, so a re-add of the
	// same ID is still refused.
	wantInt(t, do(t, c, opXlen, "s"), 0)
	wantErr(t, do(t, c, opXadd, "s", "1-1", "f", "v"),
		"ERR The ID specified in XADD is equal or smaller than the target stream top item")
}

func TestXdelMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opXdel, "nokey", "1-1"), 0)
}

func TestXdelInvalidID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	// A malformed ID fails the whole command with no partial effect.
	wantErr(t, do(t, c, opXdel, "s", "1-1", "bad"),
		"ERR Invalid stream ID specified as stream command argument")
	wantInt(t, do(t, c, opXlen, "s"), 1)
}

func TestXsetid(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	// Raise the last ID; a following auto/partial add lands above it.
	wantStatus(t, do(t, c, opXsetid, "s", "10-0"), "OK")
	wantBulk(t, do(t, c, opXadd, "s", "10-*", "f", "v"), "10-1")
	// A bare ms completes to ms-0.
	wantStatus(t, do(t, c, opXsetid, "s", "20"), "OK")
	wantBulk(t, do(t, c, opXadd, "s", "20-*", "f", "v"), "20-1")
}

func TestXsetidRejectsSmaller(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "5-5", "f", "v")
	wantErr(t, do(t, c, opXsetid, "s", "5-4"),
		"ERR The ID specified in XSETID is smaller than the target stream top item")
	// Equal to the top item is allowed.
	wantStatus(t, do(t, c, opXsetid, "s", "5-5"), "OK")
}

func TestXsetidMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXsetid, "nokey", "5-0"),
		"ERR The XSETID command requires the key to exist.")
}

func TestXsetidOptions(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	wantStatus(t, do(t, c, opXsetid, "s", "9-0", "ENTRIESADDED", "100", "MAXDELETEDID", "5-0"), "OK")
	// A bad option keyword is a syntax error.
	wantErr(t, do(t, c, opXsetid, "s", "10-0", "BOGUS", "1"), "ERR syntax error")
}

func TestObjectEncodingStream(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "s"), "stream")
}

// idLess reports whether the "ms-seq" text a precedes b.
func idLess(t *testing.T, a, b string) bool {
	t.Helper()
	return parseIDText(t, a).less(parseIDText(t, b))
}

func parseIDText(t *testing.T, s string) streamID {
	t.Helper()
	id, ok := parseStreamID([]byte(s))
	if !ok {
		t.Fatalf("unparseable id %q", s)
	}
	return id
}
