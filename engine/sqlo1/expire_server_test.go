package sqlo1

// The T7 slice 6 surface: the four expire commands with their NX/XX/
// GT/LT gates, the TTL projection family, PERSIST, DBSIZE, and the
// INFO expired/evicted counters, all over the wire.

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// sendCmd writes one RESP array command.
func sendCmd(t *testing.T, c net.Conn, parts ...string) {
	t.Helper()
	if _, err := c.Write([]byte(respCmd(parts...))); err != nil {
		t.Fatal(err)
	}
}

func TestExpireFamilySurface(t *testing.T) {
	c, r := startServer(t)
	step := func(reply string, parts ...string) {
		t.Helper()
		sendCmd(t, c, parts...)
		expect(t, r, reply)
	}

	// The absolute pair round-trips exactly through the two time
	// projections; the wall clock never enters these replies.
	step("+OK\r\n", "SET", "k", "v")
	step(":1\r\n", "PEXPIREAT", "k", "4000000000000")
	step(":4000000000000\r\n", "PEXPIRETIME", "k")
	step(":4000000000\r\n", "EXPIRETIME", "k")

	// Past the header's 42-bit horizon the deadline clamps to May 2109
	// instead of wrapping into the past.
	step(":1\r\n", "PEXPIREAT", "k", "99999999999999")
	step(":4398046511103\r\n", "PEXPIRETIME", "k")

	// Gates against an existing far-future expiry.
	step(":0\r\n", "EXPIRE", "k", "100", "NX")
	step(":0\r\n", "EXPIRE", "k", "100", "XX", "GT")
	step(":1\r\n", "EXPIRE", "k", "100", "LT")
	step(":100\r\n", "TTL", "k")

	// PERSIST clears exactly one expiry, then reads as no-TTL.
	step(":1\r\n", "PERSIST", "k")
	step(":-1\r\n", "TTL", "k")
	step(":-1\r\n", "PTTL", "k")
	step(":-1\r\n", "EXPIRETIME", "k")
	step(":0\r\n", "PERSIST", "k")

	// The no-TTL state is infinite for GT and LT.
	step(":0\r\n", "EXPIRE", "k", "100", "XX")
	step(":0\r\n", "EXPIRE", "k", "100", "GT")
	step(":1\r\n", "EXPIRE", "k", "100", "LT")
	step(":1\r\n", "EXPIRE", "k", "200", "GT")
	step(":0\r\n", "PEXPIRE", "k", "300000", "LT")
	// A repeated flag is not a syntax error, just the same gate twice.
	step(":0\r\n", "EXPIRE", "k", "100", "NX", "NX")

	// Option errors.
	step("-ERR GT and LT options at the same time are not compatible\r\n", "EXPIRE", "k", "100", "GT", "LT")
	step("-ERR NX and XX, GT or LT options at the same time are not compatible\r\n", "EXPIRE", "k", "100", "NX", "GT")
	step("-ERR Unsupported option FOO\r\n", "EXPIRE", "k", "100", "FOO")
	step("-ERR invalid expire time in 'expire' command\r\n", "EXPIRE", "k", "99999999999999999")
	step("-ERR value is not an integer or out of range\r\n", "EXPIRE", "k", "soon")

	// A past deadline deletes.
	step(":1\r\n", "PEXPIRE", "k", "-1")
	step("$-1\r\n", "GET", "k")
	step("+OK\r\n", "SET", "k2", "v")
	step(":1\r\n", "EXPIREAT", "k2", "1")
	step("$-1\r\n", "GET", "k2")

	// The whole family on a missing key.
	step(":0\r\n", "EXPIRE", "nosuch", "100")
	step(":0\r\n", "PEXPIREAT", "nosuch", "99999999999999")
	step(":-2\r\n", "TTL", "nosuch")
	step(":-2\r\n", "PTTL", "nosuch")
	step(":-2\r\n", "EXPIRETIME", "nosuch")
	step(":-2\r\n", "PEXPIRETIME", "nosuch")
	step(":0\r\n", "PERSIST", "nosuch")
}

func TestDBSizeSurface(t *testing.T) {
	c, r := startServer(t)
	step := func(reply string, parts ...string) {
		t.Helper()
		sendCmd(t, c, parts...)
		expect(t, r, reply)
	}

	step(":0\r\n", "DBSIZE")
	step("+OK\r\n", "SET", "a", "1")
	step(":1\r\n", "DBSIZE")
	step("+OK\r\n", "SET", "a", "2")
	step(":1\r\n", "DBSIZE")
	step(":2\r\n", "RPUSH", "l", "x", "y")
	step(":2\r\n", "DBSIZE")
	step(":1\r\n", "HSET", "h", "f", "v")
	step(":3\r\n", "DBSIZE")
	step(":1\r\n", "DEL", "a")
	step(":2\r\n", "DBSIZE")
	step(":2\r\n", "DEL", "l", "h")
	step(":0\r\n", "DBSIZE")

	sendCmd(t, c, "INFO")
	info := readBulk(t, r)
	for _, line := range []string{"expired_keys:", "evicted_keys:0"} {
		if !strings.Contains(info, line) {
			t.Fatalf("INFO missing %q:\n%s", line, info)
		}
	}
}

// TestTieredKeyCount pins the KeyCount equation across the tier
// transitions, including the two doc 11 section 4 tolerance windows:
// a blind overwrite of a cold key double-counts until its drain, and
// an expired key leaves the count at its tombstone, not its due time.
func TestTieredKeyCount(t *testing.T) {
	ctx := context.Background()
	tr := NewTiered(NewMemStore(), TieredConfig{
		Budget: Budget{Entries: 256, Arenas: 64 << 20},
		Seed:   7,
	})
	s, err := NewStr(tr, StrConfig{RopeMin: 64, Log2Chunk: 6})
	if err != nil {
		t.Fatal(err)
	}
	count := func(want int64, when string) {
		t.Helper()
		if got := tr.KeyCount(); got != want {
			t.Fatalf("KeyCount %s = %d, want %d", when, got, want)
		}
	}

	// Hot-only, then drained, then cold: the count never moves.
	if err := s.Set(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	// A rope is one key no matter how many plane records carry it.
	if err := s.Set(ctx, []byte("rope"), bytes.Repeat([]byte{'r'}, 300)); err != nil {
		t.Fatal(err)
	}
	count(3, "hot")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	count(3, "drained")
	tr.EvictAllForTest()
	count(3, "cold")

	// Blind overwrite of a cold key: the evicted header left a ghost
	// that remembers the store holds this key, so the fresh hot record
	// takes over the cold record's count instead of doubling it. The
	// doc 11 section 4 +1 window only opens once the ghost itself has
	// been forgotten.
	if err := s.Set(ctx, []byte("a"), []byte("3")); err != nil {
		t.Fatal(err)
	}
	count(3, "inside the overwrite window")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	count(3, "after the overwrite drained")

	// Delete of a cold key counts down at the tombstone, immediately.
	if _, err := s.Del(ctx, []byte("b")); err != nil {
		t.Fatal(err)
	}
	count(2, "cold delete pending")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	count(2, "cold delete drained")

	// Delete of a hot key.
	if err := s.Set(ctx, []byte("c"), []byte("4")); err != nil {
		t.Fatal(err)
	}
	count(3, "hot insert")
	if _, err := s.Del(ctx, []byte("c")); err != nil {
		t.Fatal(err)
	}
	count(2, "hot delete")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	count(2, "hot delete drained")

	// An expired fresh key dies in RAM: the drain's reap-cancel turns
	// it into a tombstone and the count settles at zero for it.
	if err := s.Set(ctx, []byte("e"), []byte("5")); err != nil {
		t.Fatal(err)
	}
	count(3, "doomed key hot")
	tr.SetExpireForTest([]byte("e"), time.Now().UnixMilli()-60_000)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	count(2, "doomed key reap-cancelled")
}
