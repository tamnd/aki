package list

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// BLPOP and BRPOP (spec 2064/f3/13 M3 slice 8). The suite drives the real
// handlers on a one-shard runtime and covers the three ways a blocking pop
// resolves: an immediate serve off an already non-empty key, a park that a later
// push on another connection wakes, and a park that a timeout ends with the null
// array. It also pins the reply shape at each pipeline slot (the reorder ring
// stalls a command behind an unresolved block), the FIFO order across multiple
// waiters, the sibling unlink that keeps a served multi-key waiter from waking
// twice, the timeout error texts, and WRONGTYPE probing order, all against the
// naive expectation Redis matches. A byte-exact live replay guards the wire form.

// --- the blocking harness -------------------------------------------------

const (
	bkLpush byte = iota + 1
	bkRpush
	bkLpop
	bkRpop
	bkLlen
	bkLrange
	bkSet
	bkBlpop
	bkBrpop
	bkBlmove
	bkBrpoplpush
	bkBlmpop
	bkLmove
	bkLmpop
	bkLast
)

func blockHandlers() []shard.Handler {
	h := make([]shard.Handler, bkLast)
	h[bkLpush] = Lpush
	h[bkRpush] = Rpush
	h[bkLpop] = Lpop
	h[bkRpop] = Rpop
	h[bkLlen] = Llen
	h[bkLrange] = Lrange
	h[bkBlpop] = Blpop
	h[bkBrpop] = Brpop
	h[bkBlmove] = Blmove
	h[bkBrpoplpush] = Brpoplpush
	h[bkBlmpop] = Blmpop
	h[bkLmove] = Lmove
	h[bkLmpop] = Lmpop
	h[bkSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if err := cx.St.Set(args[0], args[1]); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		r.Status("OK")
	}
	return h
}

func newBlockHarness(t *testing.T) *shard.Runtime {
	t.Helper()
	rt := shard.New(1, 8<<20, 1<<18)
	rt.Use(blockHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// park sends a blocking command routed by its first key and returns at once
// without waiting for a reply, the way a real client would after a BLPOP that
// blocks. It does not arm the reader barrier, which is a driver-side concern; the
// reorder ring alone defers the reply here.
func park(t *testing.T, c *shard.Conn, op byte, a ...string) {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(op, 0, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
}

// drainN polls the connection until it has emitted want whole replies, then
// returns them in order, so a test can read a parked reply and the commands
// pipelined behind it as separate frames.
func drainN(t *testing.T, c *shard.Conn, want int) [][]byte {
	t.Helper()
	var reps [][]byte
	deadline := time.Now().Add(10 * time.Second)
	for len(reps) < want {
		c.DrainReplies(func(b []byte) { reps = append(reps, append([]byte(nil), b...)) })
		if len(reps) < want {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %d replies, got %d", want, len(reps))
			}
			runtime.Gosched()
		}
	}
	return reps
}

func drainOne(t *testing.T, c *shard.Conn) []byte {
	t.Helper()
	return drainN(t, c, 1)[0]
}

// noReply fails if the connection emits anything within dur, the way a still
// blocked waiter must stay silent.
func noReply(t *testing.T, c *shard.Conn, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		got := false
		c.DrainReplies(func(b []byte) { got = true })
		if got {
			t.Fatal("connection emitted a reply while it should still block")
		}
		runtime.Gosched()
	}
}

// --- immediate serve ------------------------------------------------------

func TestBlpopImmediateServe(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k", "a", "b", "c"), 3)
	wantArray(t, do(t, c, bkBlpop, "k", "0"), "k", "a") // head
	wantArray(t, do(t, c, bkBrpop, "k", "0"), "k", "c") // tail
	wantArray(t, do(t, c, bkLrange, "k", "0", "-1"), "b")
}

func TestBlpopImmediateServeDropsEmptied(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k", "only"), 1)
	wantArray(t, do(t, c, bkBlpop, "k", "0"), "k", "only")
	// The key is deleted the moment its last element leaves, so LLEN is 0.
	wantInt(t, do(t, c, bkLlen, "k"), 0)
}

func TestBlpopMultiKeyFirstNonEmpty(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k2", "v"), 1)
	// k1 is missing, k2 holds a list: the first non-empty key wins.
	wantArray(t, do(t, c, bkBlpop, "k1", "k2", "k3", "0"), "k2", "v")
}

// --- park then serve ------------------------------------------------------

func TestBlpopParkThenServe(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlpop, "k", "0")
	wantInt(t, do(t, b, bkLpush, "k", "v"), 1) // push reports length before serve
	wantArray(t, drainOne(t, a), "k", "v")
}

func TestBlpopParkThenServeLaterBatch(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlpop, "k", "0")
	// An unrelated push must not wake the waiter.
	wantInt(t, do(t, b, bkRpush, "other", "x"), 1)
	noReply(t, a, 50*time.Millisecond)
	// The push on the blocked key does.
	wantInt(t, do(t, b, bkLpush, "k", "hello"), 1)
	wantArray(t, drainOne(t, a), "k", "hello")
}

func TestBlpopServesWaitersInFifoOrder(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	park(t, a1, bkBlpop, "k", "0") // parks first
	park(t, a2, bkBlpop, "k", "0") // parks second
	// Three elements arrive at once: the first waiter takes the head, the second
	// the next, the third stays in the list.
	wantInt(t, do(t, b, bkRpush, "k", "v1", "v2", "v3"), 3)
	wantArray(t, drainOne(t, a1), "k", "v1")
	wantArray(t, drainOne(t, a2), "k", "v2")
	wantArray(t, do(t, b, bkLrange, "k", "0", "-1"), "v3")
}

func TestBlpopMultiKeySiblingUnlink(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlpop, "k1", "k2", "k3", "0")
	// A push on the middle key serves the waiter and unlinks it from k1 and k3.
	wantInt(t, do(t, b, bkRpush, "k2", "v"), 1)
	wantArray(t, drainOne(t, a), "k2", "v")
	// Later pushes on the other two keys must not wake the already served waiter,
	// and the pushed values stay in place.
	wantInt(t, do(t, b, bkRpush, "k1", "w"), 1)
	wantInt(t, do(t, b, bkRpush, "k3", "z"), 1)
	noReply(t, a, 100*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, "k1", "0", "-1"), "w")
	wantArray(t, do(t, b, bkLrange, "k3", "0", "-1"), "z")
}

// TestBlpopReorderStall proves the reorder ring holds every reply pipelined
// behind an unresolved block: a command dispatched after a parked BLPOP runs,
// but its reply cannot emit until the block is served, and then both land in
// request order.
func TestBlpopReorderStall(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	// Pipeline BLPOP k and LLEN other in one flush.
	if err := a.DoAt(bkBlpop, 0, [][]byte{[]byte("k"), []byte("0")}); err != nil {
		t.Fatal(err)
	}
	if err := a.DoAt(bkLlen, 0, [][]byte{[]byte("other")}); err != nil {
		t.Fatal(err)
	}
	a.Flush()
	// Neither reply may emit while the BLPOP is parked.
	noReply(t, a, 100*time.Millisecond)
	// Serving the BLPOP releases both, in order.
	wantInt(t, do(t, b, bkRpush, "k", "v"), 1)
	reps := drainN(t, a, 2)
	wantArray(t, reps[0], "k", "v")
	wantInt(t, reps[1], 0)
}

// --- timeout --------------------------------------------------------------

func TestBlpopTimeoutNullArray(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	park(t, c, bkBlpop, "missing", "0.1") // 100 ms, no serving push
	// The armed timer fires on the owner and delivers the RESP2 null array.
	wantNil(t, drainOne(t, c))
}

func TestBlpopFractionalTimeoutFires(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	start := time.Now()
	park(t, c, bkBrpop, "missing", "0.05")
	wantNil(t, drainOne(t, c))
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("timeout fired after %v, want at least ~50ms", elapsed)
	}
}

func TestBlpopTimeoutErrors(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantErr(t, do(t, c, bkBlpop, "k", "-1"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBlpop, "k", "-0.5"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBlpop, "k", "notanumber"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlpop, "k", "nan"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlpop, "k", "inf"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlpop, "k", ""), errTimeoutFloat)
}

func TestBlpopWrongType(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantStatus(t, do(t, c, bkSet, "s", "v"), "OK")
	wantErr(t, do(t, c, bkBlpop, "s", "0"), wrongType)
	// A wrong-typed key probed before any poppable one aborts with WRONGTYPE.
	wantInt(t, do(t, c, bkRpush, "lst", "x"), 1)
	wantErr(t, do(t, c, bkBlpop, "s", "lst", "0"), wrongType)
	// A poppable key reached first serves and never probes the string.
	wantArray(t, do(t, c, bkBlpop, "lst", "s", "0"), "lst", "x")
}

// --- race cleanliness -----------------------------------------------------

// TestBlpopServeRaceClean drives the cross-goroutine wake the race detector
// guards: the owner running a push completes a reply on a foreign connection
// while that connection's reader drains, the same producer/consumer handoff
// TestCompleteBlockedConcurrent covers, now through the full BLPOP path.
func TestBlpopServeRaceClean(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlpop, "k", "0") // a is parked before b pushes
	go func() {
		_ = b.DoAt(bkRpush, 0, [][]byte{[]byte("k"), []byte("v")})
		b.Flush()
	}()
	wantArray(t, drainOne(t, a), "k", "v")
}

// --- zero-alloc park ------------------------------------------------------

// TestBlpopParkZeroAllocs pins the warm park path to zero allocations: with the
// node slab and the waiter list already resident, a park reuses a recycled node
// and a serve returns it, so the steady state a busy blocked key holds allocates
// nothing. An anchor waiter keeps the list resident across the measured runs so
// only the park and unlink are timed, not the list's first creation.
func TestBlpopParkZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("AllocsPerRun counts race-runtime allocations under -race")
	}
	g := &reg{m: make(map[string]*list), waiters: make(map[string]*waitList)}
	c := &shard.Conn{}
	keys := [][]byte{[]byte("k")}
	spec := waitSpec{kind: kindPop, front: true}
	_ = parkWaiter(g, keys, spec, c, 0) // anchor: keeps the waiter list alive
	for i := 0; i < 8; i++ {            // warm the node slab and the free stack
		g.unlinkAll(nil, parkWaiter(g, keys, spec, c, 1))
	}
	allocs := testing.AllocsPerRun(200, func() {
		g.unlinkAll(nil, parkWaiter(g, keys, spec, c, 1))
	})
	if allocs != 0 {
		t.Errorf("warm BLPOP park allocated %v times per run, want 0", allocs)
	}
}

// --- live redis parity ----------------------------------------------------

// blpopDiffer pairs the BLPOP harness with a live redis for a byte-exact replay,
// the same shape the LMPOP and move parity suites use. It replays only the cases
// a single connection resolves: an immediate serve, a first-non-empty multi-key
// serve, a short finite timeout, and the two timeout errors, so neither side
// blocks forever.
type blpopDiffer struct {
	t *testing.T
	c *shard.Conn
	r *redisConn
}

func newBlpopDiffer(t *testing.T) *blpopDiffer {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay BLPOP against a live Redis")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rc.close)
	rt := newBlockHarness(t)
	return &blpopDiffer{t: t, c: rt.NewConn(), r: rc}
}

func (d *blpopDiffer) agree(op byte, verb string, args ...string) {
	d.t.Helper()
	mine := decodeReply(d.t, do(d.t, d.c, op, args...))
	theirs, err := d.r.cmdReply(append([]string{verb}, args...)...)
	if err != nil {
		d.t.Fatalf("%s %v: redis transport error: %v", verb, args, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("%s %v: aki %v, redis %v", verb, args, render(mine), render(theirs))
	}
}

func (d *blpopDiffer) freshKey(name string) string {
	k := "aki:blpop:" + name
	d.r.cmd("DEL", k)
	return k
}

func TestBlpopAgainstRedis(t *testing.T) {
	d := newBlpopDiffer(t)

	// Immediate serve off an existing list, both ends, and the leftover.
	k := d.freshKey("basic")
	d.agree(bkRpush, "RPUSH", k, "a", "b", "c")
	d.agree(bkBlpop, "BLPOP", k, "0")
	d.agree(bkBrpop, "BRPOP", k, "0")
	d.agree(bkLrange, "LRANGE", k, "0", "-1")

	// First non-empty key across a missing one.
	k1 := d.freshKey("k1")
	k2 := d.freshKey("k2")
	d.agree(bkRpush, "RPUSH", k2, "v")
	d.agree(bkBlpop, "BLPOP", k1, k2, "0")

	// Every key missing with a short finite timeout: the null array, both ends.
	m1 := d.freshKey("m1")
	m2 := d.freshKey("m2")
	d.agree(bkBlpop, "BLPOP", m1, m2, "0.05")
	d.agree(bkBrpop, "BRPOP", m1, m2, "0.05")

	// The two timeout errors, checked before the keys are touched.
	d.agree(bkBlpop, "BLPOP", k, "-1")
	d.agree(bkBlpop, "BLPOP", k, "notafloat")
}
