package drivers

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The maxmemory evictor at the driver seam (spec 2064/f3/16 sections 6 and 7).
// The engine enforces maxmemory as a live RAM ceiling: once the shard's live
// charge crosses its 1/N budget share, the worker boundary sheds victims under
// the configured policy and credits them to evicted_keys. These tests drive that
// from the wire: set a small maxmemory and a policy, write far more data than the
// budget holds, and read back that the keyspace stayed bounded and evicted_keys
// rose, or, for the policies with no eligible victim, that nothing was shed.
//
// keys and evicted_keys are read from ONE INFO snapshot so they form a consistent
// pair: with no write in flight between them, live keys plus evicted keys equals
// exactly what was written, since eviction is the only thing removing a key here
// and every key written is distinct.

// fill200 is a 200-byte value, big enough that a few thousand keys cross a
// one-megabyte budget without needing a huge write loop.
var fill200 = strings.Repeat("x", 200)

// writeKeys sends n distinct SET commands over nc, checking each ack, so the
// keyspace grows past the budget one key per shard boundary. Each SET wakes the
// owning shard, whose boundary runs one bounded eviction pass, so growth and
// eviction interleave the way they would under a real write burst.
func writeKeys(t *testing.T, nc net.Conn, br *bufio.Reader, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		send(t, nc, "SET", "k:"+strconv.Itoa(i), fill200)
		expect(t, br, "+OK\r\n")
	}
}

// waitEvicted polls INFO until evicted_keys is at least want or the deadline
// passes, returning the last snapshot. Each poll issues INFO, which wakes the
// shard and lets a further boundary pass run, so a keyspace still above budget
// keeps shedding while we watch.
func waitEvicted(t *testing.T, nc net.Conn, br *bufio.Reader, want uint64, d time.Duration) map[string]uint64 {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		info := readInfo(t, nc, br)
		if info["evicted_keys"] >= want || time.Now().After(deadline) {
			return info
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// TestMaxmemoryEvictsUnderAllkeysLRU is the core case: a 1MB budget under
// allkeys-lru, then 20000 200-byte keys, far more than the budget holds. The
// evictor must keep the live keyspace bounded well below what was written and
// count every drop, and the live-plus-evicted total must equal the writes exactly
// because eviction is the only key remover in play.
func TestMaxmemoryEvictsUnderAllkeysLRU(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "CONFIG", "SET", "maxmemory", "1mb")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
	expect(t, br, "+OK\r\n")

	const written = 20000
	writeKeys(t, nc, br, written)

	info := waitEvicted(t, nc, br, 1, 3*time.Second)
	evicted := info["evicted_keys"]
	keys := info["keys"]
	if evicted == 0 {
		t.Fatalf("evicted_keys = 0, the evictor never engaged under a 1MB budget after %d writes", written)
	}
	if keys >= written {
		t.Fatalf("keys = %d after writing %d under a 1MB budget, want bounded below the write count", keys, written)
	}
	if keys+evicted != written {
		t.Fatalf("keys(%d) + evicted(%d) = %d, want %d: eviction should be the only key remover", keys, evicted, keys+evicted, written)
	}
}

// TestMaxmemoryAllkeysRandomEvicts checks the random policy sheds too: it has no
// score to compute, so this exercises the sample-and-drop path with the comparator
// returning a flat score. The budget is the same 1MB ceiling.
func TestMaxmemoryAllkeysRandomEvicts(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "CONFIG", "SET", "maxmemory", "1mb")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "SET", "maxmemory-policy", "allkeys-random")
	expect(t, br, "+OK\r\n")

	const written = 20000
	writeKeys(t, nc, br, written)

	info := waitEvicted(t, nc, br, 1, 3*time.Second)
	if info["evicted_keys"] == 0 {
		t.Fatalf("evicted_keys = 0 under allkeys-random after %d writes", written)
	}
	if info["keys"] >= written {
		t.Fatalf("keys = %d, want bounded below %d", info["keys"], written)
	}
}

// TestNoevictionDoesNotEvict is the deferred-OOM case stated honestly: with the
// default noeviction policy and a small maxmemory, the store keeps every key
// rather than shedding, so evicted_keys stays 0 and the keyspace holds the full
// write count. The OOM refusal that redis would raise here is its own slice; until
// it lands, noeviction means grow, not error.
func TestNoevictionDoesNotEvict(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "CONFIG", "SET", "maxmemory", "1mb")
	expect(t, br, "+OK\r\n")
	// policy stays at its noeviction seed.

	const written = 4000
	writeKeys(t, nc, br, written)

	info := readInfo(t, nc, br)
	if info["evicted_keys"] != 0 {
		t.Fatalf("evicted_keys = %d under noeviction, want 0", info["evicted_keys"])
	}
	if info["keys"] != written {
		t.Fatalf("keys = %d under noeviction, want all %d retained", info["keys"], written)
	}
}

// TestVolatileLRUNoVictimWithoutTTL proves a volatile-* policy only considers keys
// carrying a TTL: with a small budget but every key persistent, the volatile scope
// is empty, so there is no eligible victim and the keyspace grows unshed, the same
// spot redis answers OOM. This is the no-eligible-victim break in the evictor, and
// it documents that the volatile scoping is wired rather than falling through to
// evicting persistent keys.
func TestVolatileLRUNoVictimWithoutTTL(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "CONFIG", "SET", "maxmemory", "1mb")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "SET", "maxmemory-policy", "volatile-lru")
	expect(t, br, "+OK\r\n")

	const written = 4000
	writeKeys(t, nc, br, written)

	info := readInfo(t, nc, br)
	if info["evicted_keys"] != 0 {
		t.Fatalf("evicted_keys = %d under volatile-lru with no volatile keys, want 0", info["evicted_keys"])
	}
	if info["keys"] != written {
		t.Fatalf("keys = %d, want all %d retained since none carry a TTL", info["keys"], written)
	}
}

// TestConfigSetMaxmemoryNormalizes checks CONFIG SET validates and normalizes the
// eviction parameters the way redis does: a suffixed byte quantity reads back as a
// decimal byte count, a policy name round-trips, and a bad policy name is rejected
// atomically so a later good pair in the same call does not apply.
func TestConfigSetMaxmemoryNormalizes(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "CONFIG", "SET", "maxmemory", "100mb")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "GET", "maxmemory")
	expect(t, br, "*2\r\n$9\r\nmaxmemory\r\n$9\r\n104857600\r\n")

	// A bad policy name is rejected, and because validation precedes any apply,
	// the maxmemory pair in the same call must not take effect.
	send(t, nc, "CONFIG", "SET", "maxmemory", "0", "maxmemory-policy", "bogus-policy")
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading CONFIG SET error reply: %v", err)
	}
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("CONFIG SET with a bad policy = %q, want an -ERR reply", line)
	}
	send(t, nc, "CONFIG", "GET", "maxmemory")
	expect(t, br, "*2\r\n$9\r\nmaxmemory\r\n$9\r\n104857600\r\n")
}
