package shard

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The batched owner-to-loop wake edge (M10 pull-forward slice 4), driven the
// way the reactor loop drives it: one goroutine plays the event loop with a
// dirty list the owners mark into, the workers deliver one WakeLoop per
// touched loop per drain pass through the SetWriterBatchNotify seam. The
// slice's whole risk is a lost wake on this edge hanging a connection
// invisibly, so every test here runs under a watchdog deadline, like the
// notifier tests the batching extends.

// batchLoop is the reactor-loop stand-in: a mutex-guarded dirty list the
// owners mark into, and a one-slot channel standing in for the eventfd. The
// channel keeps the eventfd's level shape as long as the consumer receives
// before it swaps, which is drainWake's order (read the eventfd, then swap):
// a WakeLoop landing after the swap leaves the token set, so the next wait
// returns at once and swaps again.
type batchLoop struct {
	mu    chan struct{} // capacity-1 mutex, so tests can hold it mid-swap
	dirty []*batchConn
	ch    chan struct{}
	wakes atomic.Uint64 // WakeLoop deliveries, the batched eventfd writes
}

// batchConn pairs a Conn with its queued flag, the same dedup the reactor
// keeps per connection: racing marks fold into one dirty entry, and the loop
// clears the flag before servicing so a later mark queues fresh.
type batchConn struct {
	c      *Conn
	queued atomic.Bool
}

func newBatchLoop() *batchLoop {
	bl := &batchLoop{mu: make(chan struct{}, 1), ch: make(chan struct{}, 1)}
	return bl
}

func (bl *batchLoop) lock()   { bl.mu <- struct{}{} }
func (bl *batchLoop) unlock() { <-bl.mu }

// mark is the owner-side mark half, the reactor's markDirty shape.
func (bl *batchLoop) mark(bc *batchConn) bool {
	if !bc.queued.CompareAndSwap(false, true) {
		return false
	}
	bl.lock()
	bl.dirty = append(bl.dirty, bc)
	bl.unlock()
	return true
}

// WakeLoop is the delivery half: level-like, a redundant delivery folds into
// the already-set token exactly as a second eventfd write folds into the
// counter.
func (bl *batchLoop) WakeLoop() {
	bl.wakes.Add(1)
	select {
	case bl.ch <- struct{}{}:
	default:
	}
}

// register wires a connection to the loop through the batched seam.
func (bl *batchLoop) register(c *Conn) *batchConn {
	bc := &batchConn{c: c}
	c.SetWriterBatchNotify(bl, func() bool { return bl.mark(bc) })
	return bc
}

// wait blocks until a delivery lands or the deadline passes; false is the
// starvation verdict.
func (bl *batchLoop) wait(d time.Duration) bool {
	select {
	case <-bl.ch:
		return true
	case <-time.After(d):
		return false
	}
}

// swap takes the dirty list, clearing each entry's queued flag before the
// caller services it, drainWake's order: any mark after the clear queues
// fresh and owes a new delivery.
func (bl *batchLoop) swap() []*batchConn {
	bl.lock()
	d := bl.dirty
	bl.dirty = nil
	bl.unlock()
	for _, bc := range d {
		bc.queued.Store(false)
	}
	return d
}

// TestBatchNotifyWakeEdge is the P1 starvation watchdog on the batched seam:
// many rounds of dispatch, drain, park, block on the loop wake. A lone reply
// is the wake edge with nothing else in flight to hide behind; a lost wake
// anywhere shows up as the deadline firing with a reply still owed.
func TestBatchNotifyWakeEdge(t *testing.T) {
	const rounds = 20000

	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	bl := newBatchLoop()
	c := rt.NewConn()
	bl.register(c)
	defer c.Close()

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }

	for i := 0; i < rounds; i++ {
		got = got[:0]
		k := fmt.Sprintf("k%03d", i%251)
		if err := c.Do(opSet, true, args(k, "v")); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		for {
			c.DrainReplies(emit)
			if !c.Owes() {
				break
			}
			if c.ParkWriter() {
				if !bl.wait(10 * time.Second) {
					t.Fatalf("round %d: reply owed but no wake arrived; the batched edge starved", i)
				}
				bl.swap()
			}
		}
		if len(got) != 1 || string(got[0]) != "+OK\r\n" {
			t.Fatalf("round %d: replies %q, want one +OK", i, got)
		}
	}
	if bl.wakes.Load() == 0 {
		t.Fatal("WakeLoop never fired; the batched seam is not wired")
	}
}

// TestBatchNotifyWakeEdgePipelined runs the watchdog with multi-shard bursts,
// so several workers' pass-end deliveries race in at once and the redundant-
// delivery folding is exercised alongside the park re-check.
func TestBatchNotifyWakeEdgePipelined(t *testing.T) {
	const rounds = 2000
	const burst = 32

	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	bl := newBatchLoop()
	c := rt.NewConn()
	bl.register(c)
	defer c.Close()

	n := 0
	emit := func([]byte) { n++ }

	for i := 0; i < rounds; i++ {
		n = 0
		for j := 0; j < burst; j++ {
			if err := c.Do(opEcho, false, args(fmt.Sprintf("e%d-%d", i, j))); err != nil {
				t.Fatal(err)
			}
		}
		c.Flush()
		for {
			c.DrainReplies(emit)
			if !c.Owes() {
				break
			}
			if c.ParkWriter() {
				if !bl.wait(10 * time.Second) {
					t.Fatalf("round %d: %d of %d replies, then the batched edge starved", i, n, burst)
				}
				bl.swap()
			}
		}
		if n != burst {
			t.Fatalf("round %d: %d replies, want %d", i, n, burst)
		}
	}
}

// TestBatchNotifyManyConns runs one loop over several connections, all keyed
// to land on every shard, so a worker's drain pass regularly claims more than
// one connection and folds them into one delivery. The watchdog covers every
// connection; the delivery count is checked against the claims (deliveries
// can never exceed claims, and the whole point of the slice is that they run
// well under).
func TestBatchNotifyManyConns(t *testing.T) {
	const conns = 8
	rounds := 2000
	if testing.Short() {
		rounds = 400
	}

	rt := testRuntime(2)
	rt.Start()
	defer rt.Stop()

	bl := newBatchLoop()
	bcs := make([]*batchConn, conns)
	emitted := make([]int, conns)
	for i := range bcs {
		bcs[i] = bl.register(rt.NewConn())
	}
	defer func() {
		for _, bc := range bcs {
			bc.c.Close()
		}
	}()

	for r := 0; r < rounds; r++ {
		for i, bc := range bcs {
			emitted[i] = 0
			if err := bc.c.Do(opSet, true, args(fmt.Sprintf("m%02d", r%97), "v")); err != nil {
				t.Fatal(err)
			}
			bc.c.Flush()
		}
		for {
			owed := false
			for i, bc := range bcs {
				k := i
				bc.c.DrainReplies(func([]byte) { emitted[k]++ })
				if bc.c.Owes() {
					owed = true
				}
			}
			if !owed {
				break
			}
			parked := true
			for _, bc := range bcs {
				if bc.c.Owes() && !bc.c.ParkWriter() {
					parked = false
				}
			}
			if parked {
				if !bl.wait(10 * time.Second) {
					t.Fatalf("round %d: replies owed on a parked loop and no wake arrived", r)
				}
			}
			bl.swap()
		}
		for i, n := range emitted {
			if n != 1 {
				t.Fatalf("round %d: conn %d emitted %d replies, want 1", r, i, n)
			}
		}
	}

	claims, _ := rt.NetWakes()
	deliveries := bl.wakes.Load()
	if deliveries == 0 || claims == 0 {
		t.Fatalf("claims %d, deliveries %d; the batched seam is not wired", claims, deliveries)
	}
	if deliveries > claims {
		t.Fatalf("deliveries %d exceed claims %d; a delivery fired without a claim behind it", deliveries, claims)
	}
	t.Logf("claims %d, deliveries %d (%.2fx batching)", claims, deliveries, float64(claims)/float64(deliveries))
}

// TestBatchNotifyPassBatching drives one worker by hand and pins the slice's
// mechanism deterministically: a single drain pass that completes batches for
// several parked connections on the same loop claims each connection but
// delivers exactly one WakeLoop.
func TestBatchNotifyPassBatching(t *testing.T) {
	const conns = 8

	rt := testRuntime(1)
	// Not started: the test is the worker.
	w := rt.workers[0]

	bl := newBatchLoop()
	bcs := make([]*batchConn, conns)
	for i := range bcs {
		bcs[i] = bl.register(rt.NewConn())
		if !bcs[i].c.ParkWriter() {
			t.Fatalf("conn %d: fresh connection did not park clean", i)
		}
	}

	for i, bc := range bcs {
		if err := bc.c.Do(opSet, true, args(fmt.Sprintf("p%d", i), "v")); err != nil {
			t.Fatal(err)
		}
		bc.c.Flush()
	}

	if n := w.drainPass(); n != conns {
		t.Fatalf("drain pass executed %d commands, want %d", n, conns)
	}
	if got := bl.wakes.Load(); got != 1 {
		t.Fatalf("one pass over %d dirty conns delivered %d WakeLoops, want 1", conns, got)
	}
	if claims := w.connWakes.load(); claims != conns {
		t.Fatalf("one pass claimed %d conn wakes, want %d", claims, conns)
	}
	dirty := bl.swap()
	if len(dirty) != conns {
		t.Fatalf("dirty list has %d conns, want %d", len(dirty), conns)
	}
	for i, bc := range bcs {
		got := collect(t, bc.c, 1)
		if string(got[0]) != "+OK\r\n" {
			t.Fatalf("conn %d: reply %q, want +OK", i, got[0])
		}
	}
}

// TestBatchNotifySwapRace hammers the slice's stated cliff: an owner marking
// a connection dirty concurrently with the loop's list swap. The loop side
// spins swap-service turns with no wait between them (every interleaving of
// mark, queued-clear, and swap gets hit at -race iteration counts), and the
// invariant under test is that a connection marked after the swap still gets
// its reply: either its mark queued fresh onto the new list with a delivery
// behind it, or the loop's own park re-check caught the push. Starvation
// fails the watchdog.
func TestBatchNotifySwapRace(t *testing.T) {
	rounds := 30000
	if testing.Short() {
		rounds = 5000
	}

	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	bl := newBatchLoop()
	c := rt.NewConn()
	bl.register(c)
	defer c.Close()

	replies := 0
	emit := func([]byte) { replies++ }

	for i := 0; i < rounds; i++ {
		replies = 0
		if err := c.Do(opSet, true, args(fmt.Sprintf("r%03d", i%251), "v")); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		deadline := time.Now().Add(10 * time.Second)
		for {
			// Swap before and after the drain, racing the owner's mark from
			// both sides instead of waiting for the delivery.
			bl.swap()
			c.DrainReplies(emit)
			if !c.Owes() {
				break
			}
			bl.swap()
			if c.ParkWriter() {
				// Poll with a short wait so the loop keeps swapping against
				// in-flight marks; the deadline is the starvation verdict.
				if !bl.wait(time.Millisecond) && time.Now().After(deadline) {
					t.Fatalf("round %d: reply owed but the swap-racing loop starved", i)
				}
			}
		}
		if replies != 1 {
			t.Fatalf("round %d: %d replies, want 1", i, replies)
		}
	}
}
