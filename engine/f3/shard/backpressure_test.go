package shard

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// The block-not-drop rail (backpressure.go, spec 2064/f3/06 section 8, slice
// 5a). These drive the worker directly, the test goroutine as owner, so the park
// and the retry happen at exactly the boundaries the run loop uses.

const (
	opBpParkOnce byte = iota + 1
	opBpEcho
	opBpParkAlways
	opBpSet
)

// drainAvail takes whatever replies are ready on c right now, without waiting.
func drainAvail(c *Conn) [][]byte {
	var got [][]byte
	c.DrainReplies(func(rep []byte) {
		got = append(got, append([]byte(nil), rep...))
	})
	return got
}

// TestBackpressureParkThenComplete parks a write the first time it runs and
// completes it on retry, the core rail: the parked slot produces no reply and
// holds its whole batch (a following ECHO waits with it), then a retry re-runs
// the handler, the arena admits the write, and the node pushes with both replies
// in sequence order.
func TestBackpressureParkThenComplete(t *testing.T) {
	rt := New(1, testArena, testSeg)
	calls := 0
	rt.Use([]Handler{
		opBpParkOnce: func(cx *Ctx, args [][]byte, r Reply) {
			if calls == 0 {
				calls++
				cx.ParkFull(store.ErrFull)
				return
			}
			r.Status("OK")
		},
		opBpEcho: func(cx *Ctx, args [][]byte, r Reply) { r.Bulk(args[0]) },
	})
	c := rt.NewConn()
	w := rt.workers[0]

	if err := c.Do(opBpParkOnce, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opBpEcho, false, args("hi")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()

	// The write parked: it is on the FIFO, its batch is held, and nothing has
	// been delivered, not even the ECHO that ran fine behind it.
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	if w.bpWaits != 1 {
		t.Fatalf("bpWaits = %d, want 1", w.bpWaits)
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %d replies while a write was parked, want 0", len(got))
	}

	// Retry: the arena admits the write now, the FIFO drains, and the node
	// pushes with both replies in order.
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after retry, want 0", len(w.fullWaiters))
	}
	if w.bpStalls != 0 {
		t.Fatalf("bpStalls = %d, want 0 (the write completed, never stalled)", w.bpStalls)
	}
	got := collect(t, c, 2)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("parked reply = %q, want +OK", got[0])
	}
	if !bytes.Equal(got[1], []byte("$2\r\nhi\r\n")) {
		t.Fatalf("held ECHO reply = %q, want $2 hi", got[1])
	}
}

// TestBackpressureStallsToOOM surfaces the OOM reply when no drain progress is
// possible: a write that parks forever, with no cold region to advance the
// progress cursor, crosses the coarse stall window in exactly bpStallWindow
// retry passes and then takes store.ErrFull's own wire text, wire-identical to
// the pre-block-not-drop full-arena refusal.
func TestBackpressureStallsToOOM(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Use([]Handler{
		opBpParkAlways: func(cx *Ctx, args [][]byte, r Reply) {
			cx.ParkFull(store.ErrFull)
		},
	})
	c := rt.NewConn()
	w := rt.workers[0]

	if err := c.Do(opBpParkAlways, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}

	passes := 0
	for len(w.fullWaiters) > 0 {
		if passes > bpStallWindow+4 {
			t.Fatalf("still parked after %d passes, stall window is %d", passes, bpStallWindow)
		}
		w.retryFull()
		passes++
	}
	if passes != bpStallWindow {
		t.Fatalf("stalled out after %d passes, want exactly %d", passes, bpStallWindow)
	}
	if w.bpStalls != 1 {
		t.Fatalf("bpStalls = %d, want 1", w.bpStalls)
	}
	got := collect(t, c, 1)
	// The store here has no cold region, so the taxonomy names the tier absent:
	// the reply is ErrFull's text with the cause appended (backpressure.go
	// stallOut, store.StallReason).
	want := []byte("-ERR " + store.ErrFull.Error() + " (no larger-than-memory tier)\r\n")
	if !bytes.Equal(got[0], want) {
		t.Fatalf("stall reply = %q, want %q", got[0], want)
	}
}

// bpColdWorker builds an unstarted single-shard runtime whose store has a small
// arena and a cold region behind a tight resident cap, the shape where a
// foreground write can fail to allocate while the migrator is a drain away from
// freeing room. The test goroutine owns the worker.
func bpColdWorker(t *testing.T, arenaBytes, segBytes int, capBytes uint64) (*Conn, *worker, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(store.Options{
		ArenaBytes:       arenaBytes,
		SegBytes:         segBytes,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: capBytes,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	r := &Runtime{workers: make([]*worker, 1)}
	r.resolveConnCaps(Config{})
	w := newWorker(0, st)
	w.rt = r
	r.workers[0] = w
	r.Use([]Handler{
		opBpSet: func(cx *Ctx, a [][]byte, rep Reply) {
			if err := cx.St.Set(a[0], a[1]); err != nil {
				if cx.ParkFull(err) {
					return
				}
				rep.Err("ERR " + err.Error())
				return
			}
			rep.Status("OK")
		},
	})
	return r.NewConn(), w, st
}

// TestBackpressureNoDropUnderDrain is the L17 proof on the real migrator: flood
// distinct keys until a SET genuinely fails to allocate and parks, then run the
// cold drain and the epoch reclamation until the parked write allocates. The
// write is never acknowledged and dropped: it completes with +OK after the drain
// frees room, and its key reads back. No stall-out fires, because progress was
// always possible.
func TestBackpressureNoDropUnderDrain(t *testing.T) {
	c, w, s := bpColdWorker(t, 512<<10, 64<<10, 128<<10)
	key := func(i int) []byte { return fmt.Appendf(nil, "k:%07d", i) }
	val := func(i int) []byte { return fmt.Appendf(nil, "v-%d", i) }

	// Flood distinct keys (no overwrites, so no dead bytes the fully-dead
	// backstop could reclaim) until a write parks on a full arena. drainAndExecute
	// runs no cold drain, so the arena fills.
	parkedAt := -1
	for i := 0; i < 300000 && len(w.fullWaiters) == 0; i++ {
		if err := c.Do(opBpSet, true, [][]byte{key(i), val(i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		for _, rep := range drainAvail(c) {
			if len(rep) > 0 && rep[0] == '-' {
				t.Fatalf("write %d errored before the arena filled: %q", i, rep)
			}
		}
		if len(w.fullWaiters) > 0 {
			parkedAt = i
		}
	}
	if parkedAt < 0 {
		t.Fatal("arena never filled: no write parked, fixture too roomy")
	}
	if !s.NeedsColdDrain() {
		t.Fatal("resident charge did not cross the cap, migrator would not engage")
	}

	// Reclaim the arena the way the run loop's boundary does: retire the segments
	// phase 2 emptied, compact and reclaim them, then retry the parked write
	// against the freed space. This half never stages a drain, so it is safe to
	// run after the I/O goroutine is joined.
	reclaimRetry := func() {
		w.retireDrained()
		w.st.CompactArena()
		w.st.ReclaimSafe(w.ep.safe())
		w.retryFull()
	}
	// Phase one: with the I/O goroutine live, stage drains off the owner, run the
	// completions (phase 2 flips records cold and lists emptied segments), and
	// reclaim after each.
	for pass := 0; pass < 400 && len(w.fullWaiters) > 0 && s.NeedsColdDrain(); pass++ {
		w.drainCold()
		w.advanceIntents()
		reclaimRetry()
	}
	// Join the I/O goroutine so every staged drain has posted its completion,
	// then drain those completions and reclaim until the parked write clears. No
	// drainCold here: the jobs channel is closed.
	w.io.stop()
	for i := 0; i < 400 && (w.io.pool.out > 0 || len(w.fullWaiters) > 0); i++ {
		w.advanceIntents()
		reclaimRetry()
	}

	if len(w.fullWaiters) != 0 {
		t.Fatalf("write still parked after draining: fullWaiters = %d", len(w.fullWaiters))
	}
	if w.bpStalls != 0 {
		t.Fatalf("bpStalls = %d, want 0: the parked write should complete by drain, not stall out", w.bpStalls)
	}
	if s.Cold().Records == 0 {
		t.Fatal("no records migrated cold: the arena could not have been freed by the drain")
	}

	// The parked write completed with a real reply, not an error.
	for _, rep := range collect(t, c, 1) {
		if len(rep) > 0 && rep[0] == '-' {
			t.Fatalf("parked write ended in an error reply: %q", rep)
		}
	}
	// And its value is stored, wherever it now lives.
	got, ok := s.GetString(key(parkedAt), 0, nil)
	if !ok || string(got) != string(val(parkedAt)) {
		t.Fatalf("parked key %d = %q,%v, want %q", parkedAt, got, ok, val(parkedAt))
	}
}

// TestBackpressureReclaimPendingHoldsOffStall isolates the progress signal the
// cold-cursor-only stall check of slice 5a missed. It drives a shard into the
// exact window that flakes: a drain has moved records cold and stopped (the cold
// cursor is static and no drain is in flight), but the emptied segments are still
// queued for epoch-gated reclaim (ReclaimPending). A parked write's room is one
// reclaim pass away, so the coarse stall bound must not advance however many retry
// passes land in that window. Before the ReclaimPending signal the bound climbed
// here and OOM'd a recoverable write once it crossed the window, the L17 drop F9
// forbids.
func TestBackpressureReclaimPendingHoldsOffStall(t *testing.T) {
	c, w, s := bpColdWorker(t, 512<<10, 64<<10, 128<<10)
	key := func(i int) []byte { return fmt.Appendf(nil, "k:%07d", i) }
	val := func(i int) []byte { return fmt.Appendf(nil, "v-%d", i) }

	// Flood distinct keys until a write parks on a full arena.
	for i := 0; i < 300000 && len(w.fullWaiters) == 0; i++ {
		if err := c.Do(opBpSet, true, [][]byte{key(i), val(i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		_ = drainAvail(c)
	}
	if len(w.fullWaiters) == 0 {
		t.Fatal("arena never filled: no write parked, fixture too roomy")
	}

	// Stage drains and run their phase-2 flips, but never retire or reclaim the
	// emptied segments, so they pile up in the drained queue (ReclaimPending) while
	// the resident charge falls back under the cap (ColdDraining goes false). This
	// is the post-drain reclaim window reproduced without the timing that makes it
	// rare in CI.
	for pass := 0; pass < 600 && s.ColdDraining(); pass++ {
		w.drainCold()
		for i := 0; i < 200 && w.io.pool.out > 0; i++ {
			w.advanceIntents()
		}
	}
	w.io.stop()
	for i := 0; i < 400 && w.io.pool.out > 0; i++ {
		w.advanceIntents()
	}

	if s.ColdDraining() {
		t.Skip("shard still draining after the drain loop; window not reached on this run")
	}
	if !s.ReclaimPending() {
		t.Fatal("no emptied segments queued: the drain freed nothing, cannot exercise the window")
	}
	if len(w.fullWaiters) == 0 {
		t.Fatal("the parked write cleared during draining; nothing left to stall")
	}

	// The isolate: cold cursor static, no drain in flight, reclamation pending.
	// stallCheck must reset the bound every pass, so no stall fires across more than
	// a full window of passes.
	w.bpProg = s.ColdProgress()
	for i := 0; i < bpStallWindow*3; i++ {
		w.stallCheck()
		if w.bpStall != 0 {
			t.Fatalf("stall bound advanced to %d while reclamation was pending", w.bpStall)
		}
	}
	if w.bpStalls != 0 {
		t.Fatalf("stalled out %d writes while their room was queued for reclaim", w.bpStalls)
	}
}
