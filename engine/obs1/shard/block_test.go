package shard

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// The deferred-reply seam (handler.go Reply.Park, batch.go parked, conn.go
// CompleteBlocked and the ArmBlock/Blocked barrier): a command executes, decides
// to block, writes no reply now, and a later owner step delivers that reply at
// the command's original pipeline sequence. This slice wires the mechanism to a
// shard-package test hook only; no registered handler parks and no verb arms in
// production. The properties proven here are the transport's, the same way
// txnroute_test proves the loopback the transaction route rides.

// blockInfo is the completion target a parking handler captures: the connection
// and sequence a later CompleteBlocked delivers the reply through.
type blockInfo struct {
	conn *Conn
	seq  uint32
}

// newBlockRuntime builds a single-shard runtime whose handler table carries one
// extra op past the test surface: a keyed command that parks and reports the
// CurConn and CurSeq it captured before returning. One shard keeps sequence
// order equal to enqueue order, so the reorder assertions are exact.
func newBlockRuntime(t *testing.T) (*Runtime, byte, chan blockInfo) {
	t.Helper()
	ch := make(chan blockInfo, 256)
	handlers := testHandlers()
	blockOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		// Capture the completion target while the fields are valid, then park:
		// the reply arrives later through conn.CompleteBlocked.
		info := blockInfo{conn: cx.CurConn(), seq: cx.CurSeq()}
		r.Park()
		ch <- info
	})
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	return rt, blockOp, ch
}

// TestParkNoReplyStallsPipeline proves a parked command writes no reply and
// holds the reorder cursor: replies enqueued before it emit, its own slot and
// everything behind it do not, because the cursor cannot advance past the park.
func TestParkNoReplyStallsPipeline(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(opSet, true, args("a", "1")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(blockOp, true, args("blk")); err != nil { // seq 1
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 3
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}

	var got [][]byte
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	if len(got) != 1 {
		t.Fatalf("emitted %d replies, want only the one before the park", len(got))
	}
	if string(got[0]) != "+OK\r\n" {
		t.Fatalf("reply 0 = %q, want the SET before the park", got[0])
	}
	if info := <-ch; info.seq != 1 {
		t.Fatalf("handler captured seq %d, want 1", info.seq)
	}
}

// TestCompleteBlockedEmitsInOrder proves the loopback reply lands at the parked
// command's slot: a write before it and a read after it keep their positions
// once CompleteBlocked delivers the held reply.
func TestCompleteBlockedEmitsInOrder(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(opSet, true, args("a", "vv")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(blockOp, true, args("blk")); err != nil { // seq 1
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }
	c.DrainReplies(emit)
	if len(got) != 1 {
		t.Fatalf("before completion emitted %d replies, want only the SET", len(got))
	}

	info := <-ch
	info.conn.CompleteBlocked(info.seq, []byte(":7\r\n"))
	c.DrainReplies(emit)

	want := []string{"+OK\r\n", ":7\r\n", "$2\r\nvv\r\n"}
	if len(got) != len(want) {
		t.Fatalf("emitted %d replies, want %d", len(got), len(want))
	}
	for i, exp := range want {
		if string(got[i]) != exp {
			t.Fatalf("reply %d = %q, want %q", i, got[i], exp)
		}
	}
}

// TestRingStallsThenResumes stalls the ring with a park and a full node's worth
// of commands queued behind it, near the reorder ring's wrap point, then proves
// CompleteBlocked drains every one in order through drainParked and the wrap.
func TestRingStallsThenResumes(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	w := rt.workers[0]
	c := rt.NewConn()

	drain := func() {
		for w.drainAndExecute() > 0 {
		}
	}
	// Warm the sequence close to the reorder ring's wrap so the parked run below
	// crosses the modulo boundary.
	discard := func([]byte) {}
	for i := 0; i < replyRing-6; i++ {
		if err := c.Do(opSet, true, args("w", "x")); err != nil {
			t.Fatal(err)
		}
		if (i+1)%16 == 0 {
			c.Flush()
			drain()
			c.DrainReplies(discard)
		}
	}
	c.Flush()
	drain()
	c.DrainReplies(discard)

	if err := c.Do(blockOp, true, args("blk")); err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	const behind = batchCap + 5
	for i := 0; i < behind; i++ {
		if err := c.Do(opSet, true, args("w", "x")); err != nil {
			t.Fatal(err)
		}
	}
	c.Flush()
	drain()
	blockSeq := <-ch

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }
	c.DrainReplies(emit)
	if len(got) != 0 {
		t.Fatalf("emitted %d replies behind the park, want 0", len(got))
	}

	blockSeq.conn.CompleteBlocked(blockSeq.seq, []byte(":99\r\n"))
	c.DrainReplies(emit)
	if len(got) != behind+1 {
		t.Fatalf("resumed with %d replies, want %d", len(got), behind+1)
	}
	if string(got[0]) != ":99\r\n" {
		t.Fatalf("reply 0 = %q, want the completed block", got[0])
	}
	for i := 1; i < len(got); i++ {
		if string(got[i]) != "+OK\r\n" {
			t.Fatalf("reply %d = %q, want +OK", i, got[i])
		}
	}
}

// TestBlockedGate proves the reader-side barrier tracks a parked command: it
// arms true, stays true while the park holds the emit watermark, and disarms
// itself the moment CompleteBlocked lets the watermark cross it.
func TestBlockedGate(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(opSet, true, args("a", "1")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(blockOp, true, args("blk")); err != nil { // seq 1
		t.Fatal(err)
	}
	c.ArmBlock()
	if !c.Blocked() {
		t.Fatal("barrier is not armed right after ArmBlock")
	}
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}

	discard := func([]byte) {}
	c.DrainReplies(discard) // emits the SET, the park stalls the rest
	if !c.Blocked() {
		t.Fatal("barrier disarmed while the parked reply is still owed")
	}

	info := <-ch
	info.conn.CompleteBlocked(info.seq, []byte(":1\r\n"))
	c.DrainReplies(discard)
	if c.Blocked() {
		t.Fatal("barrier still armed after the parked reply emitted")
	}
}

// TestBlockedGateInline drives one connection from a single goroutine, the
// SetInlineDrain shape, and watches the same gate held then released: the sole
// reader/writer observes Blocked true across the park and false once it plays the
// owner step and completes the reply.
func TestBlockedGateInline(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }
	c.SetInlineDrain(emit)

	if err := c.Do(opSet, true, args("a", "1")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(blockOp, true, args("blk")); err != nil { // seq 1
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2
		t.Fatal(err)
	}
	c.Flush()

	drainUntil := func(want int) {
		deadline := time.Now().Add(5 * time.Second)
		for len(got) < want {
			c.DrainReplies(emit)
			if len(got) < want && time.Now().After(deadline) {
				t.Fatalf("timed out with %d of %d replies", len(got), want)
			}
		}
	}
	drainUntil(1) // the SET; the park holds the rest
	if !c.Blocked() {
		t.Fatal("inline reader sees the gate open while the park is held")
	}

	info := <-ch
	info.conn.CompleteBlocked(info.seq, []byte(":1\r\n"))
	drainUntil(3)
	if c.Blocked() {
		t.Fatal("inline reader sees the gate held after the reply emitted")
	}
}

// TestCompleteBlockedConcurrent races a completion producer against the writer
// drain: one goroutine completes a batch of parked sequences in reverse order
// while another drains through collect, and the reorder ring must still yield
// every reply in sequence order. Meaningful under the race detector.
func TestCompleteBlockedConcurrent(t *testing.T) {
	rt, blockOp, ch := newBlockRuntime(t)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	const k = 50
	for i := 0; i < k; i++ {
		if err := c.Do(blockOp, true, args(fmt.Sprintf("b%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	c.Flush()

	// Wait for every command to park, keyed by its captured sequence.
	seen := make([]bool, k)
	for i := 0; i < k; i++ {
		info := <-ch
		if int(info.seq) >= k || seen[info.seq] {
			t.Fatalf("unexpected parked seq %d", info.seq)
		}
		seen[info.seq] = true
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := k - 1; i >= 0; i-- {
			c.CompleteBlocked(uint32(i), []byte(fmt.Sprintf(":%d\r\n", i)))
		}
	}()
	got := collect(t, c, k)
	wg.Wait()

	for i := 0; i < k; i++ {
		want := fmt.Sprintf(":%d\r\n", i)
		if string(got[i]) != want {
			t.Fatalf("reply %d = %q, want %q", i, got[i], want)
		}
	}
}

// TestParkPathZeroAllocs pins the seam to the F7 discipline: once the free list,
// the reply buffers, and the parked slice are warm, a full cycle of enqueue,
// park, drain, complete, and in-order emit allocates nothing, so the point-op
// path pays only the mirrored flag check.
func TestParkPathZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation accounting is not meaningful under the race detector")
	}
	handlers := testHandlers()
	blockOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) { r.Park() })
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	c := rt.NewConn()
	w := rt.workers[0]

	blkArgs := [][]byte{[]byte("blk")}
	rep := []byte(":1\r\n")
	discard := func([]byte) {}

	run := func() {
		s := c.seq
		if err := c.Do(blockOp, true, blkArgs); err != nil {
			t.Error(err)
		}
		c.ArmBlock()
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		c.DrainReplies(discard) // parks, emits nothing
		c.CompleteBlocked(s, rep)
		c.DrainReplies(discard) // the loopback reply lands at s
	}

	// Warm the free list, the parked slice, and the loopback node.
	for i := 0; i < 4; i++ {
		run()
	}
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("park path allocates %.1f allocs/op, want 0", allocs)
	}
}
