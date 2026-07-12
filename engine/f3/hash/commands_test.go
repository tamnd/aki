package hash

import (
	"strconv"
	"strings"
	"testing"
)

// The command-level suite: the wire behavior of every point verb this slice
// ships, driven through the runtime harness (spec 2064/f3/10 section 7). Reply
// shapes, WRONGTYPE, the empty-key drop, arity, and the OBJECT ENCODING band
// transition are all checked here; the live differential in redis_test.go replays
// the same shapes against a real Redis.

func TestHsetAndHget(t *testing.T) {
	c := newHarness(t).NewConn()
	// Three new fields in one HSET.
	wantInt(t, do(t, c, opHset, "h", "a", "1", "b", "2", "c", "3"), 3)
	// One new plus two overwrites: only the new one counts.
	wantInt(t, do(t, c, opHset, "h", "a", "9", "b", "8", "d", "4"), 1)
	wantBulk(t, do(t, c, opHget, "h", "a"), "9")
	wantBulk(t, do(t, c, opHget, "h", "d"), "4")
	wantNil(t, do(t, c, opHget, "h", "missing"))
	wantNil(t, do(t, c, opHget, "nokey", "a"))
	wantInt(t, do(t, c, opHlen, "h"), 4)
	// An empty value is a distinct present field, not a nil.
	wantInt(t, do(t, c, opHset, "h", "e", ""), 1)
	wantBulk(t, do(t, c, opHget, "h", "e"), "")
	wantInt(t, do(t, c, opHstrlen, "h", "e"), 0)
	wantInt(t, do(t, c, opHstrlen, "h", "a"), 1)
	wantInt(t, do(t, c, opHstrlen, "h", "missing"), 0)
}

func TestHsetOddArgs(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opHset, "h", "a", "1", "b"),
		"ERR wrong number of arguments for 'hset' command")
}

func TestHmset(t *testing.T) {
	c := newHarness(t).NewConn()
	wantStatus(t, do(t, c, opHmset, "h", "a", "1", "b", "2"), "OK")
	wantBulk(t, do(t, c, opHget, "h", "b"), "2")
	wantErr(t, do(t, c, opHmset, "h", "a", "1", "b"),
		"ERR wrong number of arguments for 'hmset' command")
}

func TestHsetnx(t *testing.T) {
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opHsetnx, "h", "a", "1"), 1) // new
	wantInt(t, do(t, c, opHsetnx, "h", "a", "2"), 0) // exists, not overwritten
	wantBulk(t, do(t, c, opHget, "h", "a"), "1")
	wantInt(t, do(t, c, opHsetnx, "h", "b", "3"), 1)
	wantInt(t, do(t, c, opHlen, "h"), 2)
}

func TestHmget(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1", "b", "2")
	// Array exactly as long as the request, nil per absent, repeats repeated.
	wantArray(t, do(t, c, opHmget, "h", "a", "x", "b", "a"), "1", nilElem, "2", "1")
	// A missing key answers all-nil, same length as the request.
	wantArray(t, do(t, c, opHmget, "nokey", "a", "b"), nilElem, nilElem)
}

func TestHexists(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1")
	wantInt(t, do(t, c, opHexists, "h", "a"), 1)
	wantInt(t, do(t, c, opHexists, "h", "b"), 0)
	wantInt(t, do(t, c, opHexists, "nokey", "a"), 0)
}

// HDEL removes fields, and the last field removed drops the key.
func TestHdelDropsKey(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1", "b", "2", "c", "3")
	wantInt(t, do(t, c, opHdel, "h", "a", "x", "b"), 2) // x absent
	wantInt(t, do(t, c, opHlen, "h"), 1)
	wantInt(t, do(t, c, opHdel, "h", "c"), 1)
	// The key is gone: HLEN 0, HGET nil, and OBJECT ENCODING answers nil (redis
	// 8.8.0 returns a null bulk for a key that exists nowhere).
	wantInt(t, do(t, c, opHlen, "h"), 0)
	wantNil(t, do(t, c, opHget, "h", "c"))
	wantNil(t, doAt(t, c, opObject, 1, "ENCODING", "h"))
	// HDEL on a missing key is 0.
	wantInt(t, do(t, c, opHdel, "nokey", "a"), 0)
}

// Every point verb answers WRONGTYPE on a key the string store owns.
func TestWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	wantStatus(t, do(t, c, opSet, "s", "v"), "OK")
	const wt = wrongType
	wantErr(t, do(t, c, opHset, "s", "a", "1"), wt)
	wantErr(t, do(t, c, opHmset, "s", "a", "1"), wt)
	wantErr(t, do(t, c, opHsetnx, "s", "a", "1"), wt)
	wantErr(t, do(t, c, opHget, "s", "a"), wt)
	wantErr(t, do(t, c, opHmget, "s", "a"), wt)
	wantErr(t, do(t, c, opHdel, "s", "a"), wt)
	wantErr(t, do(t, c, opHexists, "s", "a"), wt)
	wantErr(t, do(t, c, opHlen, "s"), wt)
	wantErr(t, do(t, c, opHstrlen, "s", "a"), wt)
	wantErr(t, do(t, c, opHincrby, "s", "a", "1"), wt)
	wantErr(t, do(t, c, opHincrbyfloat, "s", "a", "1"), wt)
}

// OBJECT ENCODING tracks the band: listpack inline, hashtable after a threshold
// write promotes the hash.
func TestObjectEncodingTransition(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")
	// Grow to the 512 entry cap, still listpack. The first HSET already seated one
	// field, so one field short of the cap keeps it inline.
	for i := 0; i < maxListpackEntries-1; i++ {
		do(t, c, opHset, "h", "f"+strconv.Itoa(i), "v")
	}
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")
	// One more field crosses the cap: hashtable.
	do(t, c, opHset, "h", "one-more", "v")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "hashtable")

	// A wide value promotes on its own.
	do(t, c, opHset, "big", "f", strings.Repeat("x", maxListpackValue+1))
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "big"), "hashtable")

	// A key the string store owns reports its string encoding, and a key that
	// exists nowhere answers nil (redis 8.8.0 returns a null bulk, not an error),
	// both through the delegation chain.
	do(t, c, opSet, "s", "12345")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "s"), "int")
	wantNil(t, doAt(t, c, opObject, 1, "ENCODING", "gone"))
}

// The inline band holds up to 512 fields, past what a single count byte can
// hold, so the blob header is a two-byte count. This drives the inline band well
// past 255 fields and checks that HLEN and the encoding stay correct: a one-byte
// count would have wrapped at 256 and both the length and the conversion trigger
// would have broken. Then one field past the cap flips it to native.
func TestInlineCountBeyondByte(t *testing.T) {
	c := newHarness(t).NewConn()
	for i := 0; i < 400; i++ {
		do(t, c, opHset, "h", "f"+strconv.Itoa(i), "v")
	}
	// 400 > 255: a uint8 count would report 400-256 = 144 here.
	wantInt(t, do(t, c, opHlen, "h"), 400)
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")
	// Fill exactly to the 512 cap, still inline.
	for i := 400; i < maxListpackEntries; i++ {
		do(t, c, opHset, "h", "f"+strconv.Itoa(i), "v")
	}
	wantInt(t, do(t, c, opHlen, "h"), maxListpackEntries)
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")
	// Field 513 crosses the cap and promotes to the native table.
	do(t, c, opHset, "h", "one-more", "v")
	wantInt(t, do(t, c, opHlen, "h"), maxListpackEntries+1)
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "hashtable")
}

// A hash driven all the way across the band boundary through the wire keeps
// answering point commands correctly on the native side.
func TestPointOpsAfterPromotion(t *testing.T) {
	c := newHarness(t).NewConn()
	const n = 600 // past the 512 entry cap, so the hash is native
	for i := 0; i < n; i++ {
		do(t, c, opHset, "h", "f"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "hashtable")
	wantInt(t, do(t, c, opHlen, "h"), n)
	wantBulk(t, do(t, c, opHget, "h", "f150"), "v150")
	wantInt(t, do(t, c, opHexists, "h", "f599"), 1)
	wantInt(t, do(t, c, opHexists, "h", "f600"), 0)
	wantInt(t, do(t, c, opHstrlen, "h", "f150"), 4)
	wantArray(t, do(t, c, opHmget, "h", "f0", "absent", "f599"), "v0", nilElem, "v599")
}
