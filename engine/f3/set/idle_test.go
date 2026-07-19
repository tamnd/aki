package set

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// TestSetIdleClock drives the per-key access clock the set arm of OBJECT IDLETIME
// reads: live stamps it on a real command access, and the read-only probes (the
// IDLETIME query itself, Has behind EXISTS and TYPE) leave it alone, so idle grows
// in whole seconds against the batch clock and only a real access resets it. The
// clock rides the struct padding at zero added bytes, so this is the whole cost of
// the feature on a set. The exact-second math mirrors the string idle_test; here it
// runs over the collection clock and the touch/NOTOUCH split.
func TestSetIdleClock(t *testing.T) {
	const sec = int64(1000) // one second in ms
	cx := &shard.Ctx{St: store.New(16<<20, 0), NowMs: sec}
	g := registry(cx)
	addKey(g, "s", "a", "b")
	key := []byte("s")

	// A real command access stamps the clock at the current second.
	if g.live(cx, key) == nil {
		t.Fatal("set should be live")
	}
	if idle, ok := IdleSeconds(cx, key); !ok || idle != 0 {
		t.Fatalf("idle right after access = %d, ok %v; want 0, true", idle, ok)
	}

	// Five seconds on the idle is exactly five, and reading it does not reset it:
	// the query is NOTOUCH, so a second read still reports five.
	cx.NowMs = 6 * sec
	if idle, _ := IdleSeconds(cx, key); idle != 5 {
		t.Fatalf("idle after 5s = %d; want 5", idle)
	}
	if idle, _ := IdleSeconds(cx, key); idle != 5 {
		t.Fatalf("idle query reset the clock: second read = %d; want 5", idle)
	}

	// Has is NOTOUCH too, so the presence probe behind EXISTS and TYPE does not
	// reset the idle.
	if !Has(cx, key) {
		t.Fatal("Has should see the set")
	}
	if idle, _ := IdleSeconds(cx, key); idle != 5 {
		t.Fatalf("Has reset the clock: idle = %d; want 5", idle)
	}

	// A real command access restamps, so the idle drops back to zero.
	g.live(cx, key)
	if idle, _ := IdleSeconds(cx, key); idle != 0 {
		t.Fatalf("idle after a fresh access = %d; want 0", idle)
	}

	// A missing key reports ok=false so the dispatcher falls through to the next
	// keyspace and finally the null bulk.
	if _, ok := IdleSeconds(cx, []byte("nope")); ok {
		t.Fatal("missing key should report ok=false")
	}

	// The clock folds through the same sixteen-bit wrap the string clock uses:
	// 65540s past the stamp reads back the wrapped 4, the documented fidelity price
	// of holding the clock in the struct padding rather than spending bytes.
	g.live(cx, key) // restamp at NowMs = 6s, clock = 6
	cx.NowMs = 6*sec + 65540*sec
	if idle, _ := IdleSeconds(cx, key); idle != 4 {
		t.Fatalf("idle 65540s past the stamp = %d; want the wrapped 4", idle)
	}
}

// TestSetInstallStampsClock pins the create-path half of the clock: a set placed
// through install (the funnel SADD, the *STORE result, SMOVE's destination, and
// WAL replay all share) is stamped at creation, so a brand-new key reads idle zero
// before any live access and then accrues from creation, the way Redis stamps
// robj.lru in createObject. Without the create-time stamp the clock would sit at
// zero and the idle read would report a near-full wrap of bogus idle time.
func TestSetInstallStampsClock(t *testing.T) {
	const sec = int64(1000)
	cx := &shard.Ctx{St: store.New(16<<20, 0), NowMs: 7 * sec}
	g := registry(cx)
	fresh := []byte("fresh")
	g.install(cx, fresh, newSet([]byte("m")))

	// Idle zero right after creation, with no intervening live access.
	if idle, ok := IdleSeconds(cx, fresh); !ok || idle != 0 {
		t.Fatalf("idle right after install = %d, ok %v; want 0, true", idle, ok)
	}
	// It accrues from creation: three seconds on reads exactly three.
	cx.NowMs = 10 * sec
	if idle, _ := IdleSeconds(cx, fresh); idle != 3 {
		t.Fatalf("idle 3s after install = %d; want 3", idle)
	}
}
