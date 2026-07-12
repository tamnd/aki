package stream

import (
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The XTRIM suite (spec 2064/f3/14 section 6.6): MAXLEN and MINID, the exact and
// approximate modes, LIMIT, and both bands. The native cases lean on the block
// geometry (128 entries per block, the entry cap binding well before the 4 KiB
// budget for one-byte values) so the whole-block drop boundaries are predictable,
// and they read the stream back afterwards to pin that the directory survives the
// front drop through the base offset.

// fillNative adds ids "1-0".."n-0" with a fixed schema, which packs 128 entries
// per block, and returns the connection. n above the inline cap forces the native
// band.
func fillNative(t *testing.T, n int) *shard.Conn {
	t.Helper()
	c := newHarness(t).NewConn()
	for i := 1; i <= n; i++ {
		id := strconv.Itoa(i) + "-0"
		do(t, c, opXadd, "s", id, "f", "v")
	}
	return c
}

func TestXtrimMaxlenExact(t *testing.T) {
	c := fillNative(t, 300) // blocks [1..128] [129..256] [257..300]
	// Exact keeps precisely 100: block [1..128] drops whole (172 >= 100), then the
	// boundary block tombstones ids 129..200 to reach the count.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "100"), 200)
	wantInt(t, do(t, c, opXlen, "s"), 100)
	wantEntries(t, do(t, c, opXrange, "s", "-", "201-0"),
		e("201-0", "f", "v"))
	// The survivors are 201..300, oldest-first and complete.
	got := do(t, c, opXrange, "s", "-", "+")
	assertIDRun(t, got, 201, 300)
}

func TestXtrimMaxlenApprox(t *testing.T) {
	c := fillNative(t, 300)
	// Approximate drops only whole front blocks that keep the result at or above
	// the threshold: block [1..128] goes (172 >= 100), [129..256] would drop to 44,
	// so it stays. 172 survive, more than 100, the price of never re-encoding.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "~", "100"), 128)
	wantInt(t, do(t, c, opXlen, "s"), 172)
	assertIDRun(t, do(t, c, opXrange, "s", "-", "+"), 129, 300)
}

func TestXtrimMinidExact(t *testing.T) {
	c := fillNative(t, 300)
	// Remove every id below 150: block [1..128] drops whole, then ids 129..149 in
	// the boundary block are tombstoned. 149 removed, 150..300 survive.
	wantInt(t, do(t, c, opXtrim, "s", "MINID", "150"), 149)
	wantInt(t, do(t, c, opXlen, "s"), 151)
	assertIDRun(t, do(t, c, opXrange, "s", "-", "+"), 150, 300)
}

func TestXtrimMinidApprox(t *testing.T) {
	c := fillNative(t, 300)
	// Approximate MINID drops only fully-below blocks: [1..128] goes, [129..256]
	// straddles 150 so it stays with its sub-150 ids intact.
	wantInt(t, do(t, c, opXtrim, "s", "MINID", "~", "150"), 128)
	wantInt(t, do(t, c, opXlen, "s"), 172)
	assertIDRun(t, do(t, c, opXrange, "s", "-", "+"), 129, 300)
}

func TestXtrimLimitCapsWholeBlocks(t *testing.T) {
	c := fillNative(t, 300)
	// LIMIT 200 leaves room for one 128-entry block but not two (256 > 200), so
	// exactly one front block drops even though more qualify.
	wantInt(t, do(t, c, opXtrim, "s", "MINID", "~", "300", "LIMIT", "200"), 128)
	wantInt(t, do(t, c, opXlen, "s"), 172)
	// A LIMIT below a block's live count drops nothing: no block fits under it.
	c2 := fillNative(t, 300)
	wantInt(t, do(t, c2, opXtrim, "s", "MAXLEN", "~", "1", "LIMIT", "100"), 0)
	wantInt(t, do(t, c2, opXlen, "s"), 300)
}

func TestXtrimThenAppendReads(t *testing.T) {
	c := fillNative(t, 300)
	// Drop the front block, then keep appending: the directory must still seek the
	// right block through the base offset, for both the new insert and old blocks.
	do(t, c, opXtrim, "s", "MINID", "~", "150")
	do(t, c, opXadd, "s", "400-0", "f", "v")
	do(t, c, opXadd, "s", "401-0", "f", "v")
	wantInt(t, do(t, c, opXlen, "s"), 174)
	// A bound in the dropped region still reads from the first survivor.
	assertIDRunPlus(t, do(t, c, opXrange, "s", "50", "+"), 129, 300, 400, 401)
	// A window fully inside the surviving blocks.
	wantEntries(t, do(t, c, opXrange, "s", "255-0", "258-0"),
		e("255-0", "f", "v"), e("256-0", "f", "v"), e("257-0", "f", "v"), e("258-0", "f", "v"))
}

func TestXtrimMaxlenZeroEmpties(t *testing.T) {
	c := fillNative(t, 300)
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "0"), 300)
	wantInt(t, do(t, c, opXlen, "s"), 0)
	// The key stays; a fresh append lands above the old top.
	do(t, c, opXadd, "s", "500-0", "f", "v")
	wantInt(t, do(t, c, opXlen, "s"), 1)
	wantEntries(t, do(t, c, opXrange, "s", "-", "+"), e("500-0", "f", "v"))
}

func TestXtrimInline(t *testing.T) {
	c := newHarness(t).NewConn()
	for i := 1; i <= 5; i++ {
		do(t, c, opXadd, "s", strconv.Itoa(i)+"-0", "f", strconv.Itoa(i))
	}
	// A small stream stays inline; MAXLEN front-splices the blob to the newest 3.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "3"), 2)
	wantEntries(t, do(t, c, opXrange, "s", "-", "+"),
		e("3-0", "f", "3"), e("4-0", "f", "4"), e("5-0", "f", "5"))
	// MINID on the inline blob drops the sub-threshold prefix.
	wantInt(t, do(t, c, opXtrim, "s", "MINID", "5-0"), 2)
	wantEntries(t, do(t, c, opXrange, "s", "-", "+"), e("5-0", "f", "5"))
	// OBJECT ENCODING still reports stream, the band stays invisible.
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "s"), "stream")
}

func TestXtrimMissingAndWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	// A missing key removes nothing.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "10"), 0)
	// A string key is a WRONGTYPE.
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXtrim, "k", "MAXLEN", "10"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}

func TestXtrimSyntax(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// Unknown strategy.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLENGTH", "10"), "ERR syntax error")
	// MAXLEN with a non-integer threshold.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN", "x"),
		"ERR value is not an integer or out of range")
	// MINID with a malformed ID.
	wantErr(t, do(t, c, opXtrim, "s", "MINID", "1-x"), errInvalidID)
	// LIMIT without ~.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN", "10", "LIMIT", "5"),
		"ERR syntax error, LIMIT cannot be used without the special ~ option")
	// A trailing argument past a well-formed clause.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN", "10", "junk"), "ERR syntax error")
	// LIMIT with no value.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN", "~", "10", "LIMIT"), "ERR syntax error")
	// LIMIT with a non-integer count.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN", "~", "10", "LIMIT", "x"),
		"ERR value is not an integer or out of range")
	// A strategy keyword with nothing after it.
	wantErr(t, do(t, c, opXtrim, "s", "MAXLEN"), "ERR syntax error")
}

func TestXtrimInlineNoopAndLimit(t *testing.T) {
	c := newHarness(t).NewConn()
	for i := 1; i <= 6; i++ {
		do(t, c, opXadd, "s", strconv.Itoa(i)+"-0", "f", "v")
	}
	// A threshold at or above the length removes nothing.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "10"), 0)
	wantInt(t, do(t, c, opXlen, "s"), 6)
	// LIMIT caps the inline splice too: only two of the four over-threshold entries go.
	wantInt(t, do(t, c, opXtrim, "s", "MAXLEN", "~", "2", "LIMIT", "2"), 2)
	assertIDRun(t, do(t, c, opXrange, "s", "-", "+"), 3, 6)
}

// assertIDRun checks an XRANGE reply is exactly the ids lo-0..hi-0, oldest-first,
// each carrying the fixed "f"/"v" schema.
func assertIDRun(t *testing.T, raw []byte, lo, hi int) {
	t.Helper()
	want := make([]entry, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		want = append(want, e(strconv.Itoa(i)+"-0", "f", "v"))
	}
	wantEntries(t, raw, want...)
}

// assertIDRunPlus checks a run lo-0..hi-0 followed by two explicit trailing ids,
// the fixed schema throughout.
func assertIDRunPlus(t *testing.T, raw []byte, lo, hi, a, b int) {
	t.Helper()
	want := make([]entry, 0, hi-lo+3)
	for i := lo; i <= hi; i++ {
		want = append(want, e(strconv.Itoa(i)+"-0", "f", "v"))
	}
	want = append(want, e(strconv.Itoa(a)+"-0", "f", "v"), e(strconv.Itoa(b)+"-0", "f", "v"))
	wantEntries(t, raw, want...)
}
