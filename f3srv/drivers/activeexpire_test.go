package drivers

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The active-expiry cycle at the driver seam (spec 2064/f3/16 section 3). f3's
// correctness rule is lazy expiry: any read of an expired key reaps it on touch, so
// these tests never read the key under test again. What they exercise is the
// background cycle that reclaims an untouched expired key on its own: the keys are
// placed with a short TTL and then only DBSIZE and INFO are issued, neither of which
// touches a specific key, so a drop from one live key to zero is the active cycle's
// doing and nothing else.

// waitDBSize polls DBSIZE until it reaches want or the deadline passes, returning
// the last value seen. Each poll issues a command, which wakes the owning shard and
// lets its idle boundary run one bounded active-expiry sweep.
func waitDBSize(t *testing.T, nc net.Conn, br *bufio.Reader, want int, d time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		got := readInt(t, nc, br, "DBSIZE")
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// TestActiveExpireReapsUntouchedString places a string key with a short TTL, never
// reads it again, and checks the active cycle both drops it (DBSIZE falls to 0) and
// counts the reap (expired_keys rises). A lazy-only server would leave the key
// counted until something touched it, so DBSIZE would stay at 1 here.
func TestActiveExpireReapsUntouchedString(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "vk", "v", "PX", "250")
	expect(t, br, "+OK\r\n")
	if got := readInt(t, nc, br, "DBSIZE"); got != 1 {
		t.Fatalf("DBSIZE right after SET = %d, want 1", got)
	}

	if got := waitDBSize(t, nc, br, 0, 3*time.Second); got != 0 {
		t.Fatalf("DBSIZE never fell to 0, the active cycle did not reap the key: got %d", got)
	}
	if info := readInfo(t, nc, br); info["expired_keys"] < 1 {
		t.Fatalf("expired_keys = %d, want >= 1", info["expired_keys"])
	}
}

// TestActiveExpireReapsCollection proves the cross-keyspace reaper reaches the
// collection registries, not just the string store: a zset given a short key-level
// TTL is reclaimed the same way. DBSIZE reads each registry's map size without
// reaping, so the fall to 0 is the active cycle dropping the untouched zset.
func TestActiveExpireReapsCollection(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "ZADD", "zk", "1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "PEXPIRE", "zk", "250")
	expect(t, br, ":1\r\n")
	if got := readInt(t, nc, br, "DBSIZE"); got != 1 {
		t.Fatalf("DBSIZE after ZADD = %d, want 1", got)
	}

	if got := waitDBSize(t, nc, br, 0, 3*time.Second); got != 0 {
		t.Fatalf("zset was never actively reaped: DBSIZE = %d", got)
	}
}

// TestActiveExpireDebugToggle checks DEBUG SET-ACTIVE-EXPIRE actually gates the
// cycle. With it off, an untouched expired key lingers (DBSIZE holds at 1); turning
// it back on lets the same key be reaped. The defer restores the process-global
// toggle so a fatal in the disabled window cannot leave the cycle off for later
// tests.
func TestActiveExpireDebugToggle(t *testing.T) {
	_, nc, br := startServer(t)
	defer shard.SetActiveExpire(true)

	send(t, nc, "DEBUG", "SET-ACTIVE-EXPIRE", "0")
	expect(t, br, "+OK\r\n")

	send(t, nc, "SET", "dk", "v", "PX", "40")
	expect(t, br, "+OK\r\n")

	// The key is well past its 40ms deadline within this window, but with the cycle
	// off and the key never read, DBSIZE stays at 1.
	if got := waitDBSize(t, nc, br, 0, 400*time.Millisecond); got != 1 {
		t.Fatalf("DBSIZE with active expiry off = %d, want the key to linger at 1", got)
	}

	send(t, nc, "DEBUG", "SET-ACTIVE-EXPIRE", "1")
	expect(t, br, "+OK\r\n")
	if got := waitDBSize(t, nc, br, 0, 3*time.Second); got != 0 {
		t.Fatalf("DBSIZE after re-enabling active expiry = %d, want 0", got)
	}
}
