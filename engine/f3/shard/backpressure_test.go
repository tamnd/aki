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
	opBpMSet
	opBpMSetStall
)

// msetProbe backs a synthetic MSET sub-command that mirrors str.MSetShard's park
// and resume shape (loop from ResumeIndex, ParkFullAt the failing pair, FanOK on
// completion), so the rail's fan-sub park path can be driven deterministically
// without a real store crossing its resident cap mid-command.
type msetProbe struct {
	writes      map[string]int // key -> times committed, to catch a re-applied prefix
	parkAt      int            // arg index at which the first pass parks
	parked      bool           // whether it has parked once already
	parkResume  int            // ResumeIndex observed on the parking pass
	doneResume  int            // ResumeIndex observed on the completing pass
	doneReached bool
}

// msetHandler runs the probe: it commits each pair into writes, parks once at
// parkAt on store.ErrFull recording where it resumed from, and answers FanOK when
// it reaches the end.
func msetHandler(p *msetProbe) Handler {
	return func(cx *Ctx, a [][]byte, r Reply) {
		start := cx.ResumeIndex()
		for i := start; i+1 < len(a); i += 2 {
			if i == p.parkAt && !p.parked {
				p.parked = true
				p.parkResume = start
				cx.ParkFullAt(store.ErrFull, i)
				return
			}
			p.writes[string(a[i])]++
		}
		p.doneResume = start
		p.doneReached = true
		r.FanOK()
	}
}

// TestBackpressureFanParkResumesAndFolds parks a multi-key fan sub-command
// part-way through its pairs and completes it on retry, the slice-2b rail. Three
// properties: the parked sub produces no folded reply while it waits; the retry
// resumes at the pair it parked on, not from zero, so the committed prefix is
// written exactly once (a re-applied prefix would strand dead bytes and re-consume
// the freed space); and the delayed partial folds through mergeFan into the single
// +OK the coordinator emits.
func TestBackpressureFanParkResumesAndFolds(t *testing.T) {
	p := &msetProbe{writes: map[string]int{}, parkAt: 4}
	rt := New(1, testArena, testSeg)
	rt.Use([]Handler{opBpMSet: msetHandler(p)})
	c := rt.NewConn()
	w := rt.workers[0]

	keys := args("k0", "k1", "k2", "k3", "k4", "k5")
	vals := args("v0", "v1", "v2", "v3", "v4", "v5")
	if err := c.DoFan(opBpMSet, FanOK, keys, vals); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()

	// The sub-command parked mid-pairs: one waiter, its batch held, and nothing
	// delivered, so the coordinator has folded no partial yet.
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %d replies while the fan sub was parked, want 0", len(got))
	}
	if p.parkResume != 0 {
		t.Fatalf("first pass resumed at %d, want 0 (a fresh sub starts at pair 0)", p.parkResume)
	}
	// The prefix before parkAt is committed; the parked pair and its suffix are not.
	for _, k := range []string{"k0", "k1"} {
		if p.writes[k] != 1 {
			t.Fatalf("prefix key %s committed %d times before park, want 1", k, p.writes[k])
		}
	}
	if p.writes["k2"] != 0 {
		t.Fatalf("parked pair k2 committed %d times before retry, want 0", p.writes["k2"])
	}

	// Retry: the sub resumes at the parked pair, finishes the suffix, and folds.
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after retry, want 0", len(w.fullWaiters))
	}
	if !p.doneReached {
		t.Fatal("sub-command never reached its FanOK after retry")
	}
	if p.doneResume != p.parkAt {
		t.Fatalf("retry resumed at %d, want %d (the parked pair)", p.doneResume, p.parkAt)
	}
	if w.bpStalls != 0 {
		t.Fatalf("bpStalls = %d, want 0 (the sub completed, never stalled)", w.bpStalls)
	}
	// Every pair committed exactly once: the resume skipped the committed prefix.
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4", "k5"} {
		if p.writes[k] != 1 {
			t.Fatalf("key %s committed %d times across park+retry, want exactly 1", k, p.writes[k])
		}
	}
	// The delayed partial folded into one +OK, framed once by the gather.
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("folded MSET reply = %q, want +OK", got[0])
	}
}

// TestBackpressureFanStallErrorsWholeCommand surfaces the OOM reply through the
// fan gather when a sub-command can never allocate: stallOut writes the taxonomy
// text as a FanOK error partial, not a framed reply, so mergeFan frames it exactly
// once into the command's error. A framed Err at the waiter would be double-framed
// here, so the single leading dash is the property under test.
func TestBackpressureFanStallErrorsWholeCommand(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Use([]Handler{
		opBpMSetStall: func(cx *Ctx, a [][]byte, r Reply) { cx.ParkFullAt(store.ErrFull, 0) },
	})
	c := rt.NewConn()
	w := rt.workers[0]

	if err := c.DoFan(opBpMSetStall, FanOK, args("k0", "k1"), args("v0", "v1")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}

	for passes := 0; len(w.fullWaiters) > 0; passes++ {
		if passes > bpStallWindow+4 {
			t.Fatalf("fan sub still parked after %d passes, window is %d", passes, bpStallWindow)
		}
		w.retryFull()
	}
	if w.bpStalls != 1 {
		t.Fatalf("bpStalls = %d, want 1", w.bpStalls)
	}
	got := collect(t, c, 1)
	want := []byte("-ERR " + store.ErrFull.Error() + " (no larger-than-memory tier)\r\n")
	if !bytes.Equal(got[0], want) {
		t.Fatalf("stalled MSET reply = %q, want %q", got[0], want)
	}
	if len(got[0]) > 1 && got[0][1] == '-' {
		t.Fatalf("stall partial was double-framed: %q", got[0])
	}
}

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
	// Phase one: with the I/O goroutine live, keep draining off the owner while
	// the write is parked. drainCold takes the deep backpressure trigger while a
	// write is parked (worker.go, the F9 rail): it stays engaged past the
	// migrator's low-water until a whole segment frees and the retry serves the
	// write, rather than stopping at the low-water with the write still parked.
	// Each pass runs its completions to a fixpoint (phase 2 flips records cold and
	// lists the emptied segments) and reclaims before the retry.
	for pass := 0; pass < 4000 && len(w.fullWaiters) > 0; pass++ {
		w.drainCold()
		for w.advanceIntents() > 0 {
		}
		reclaimRetry()
	}
	// Join the I/O goroutine so any last staged drain has posted its completion,
	// then run all of them to a fixpoint and reclaim to a fixpoint before the
	// final retry. No drainCold here: the jobs channel is closed. Reclaiming
	// fully before the retry keeps the coarse stall bound, a guard against a
	// genuinely stuck migrator, from being charged for a still-in-progress
	// deterministic drain on a loaded runner: with every emptied segment already
	// back on the free list the retry sees at most one no-progress pass.
	w.io.stop()
	for w.io.pool.out > 0 {
		w.advanceIntents()
	}
	for i := 0; i < 4000; i++ {
		w.retireDrained()
		freed := w.st.CompactArena()
		freed += w.st.ReclaimSafe(w.ep.safe())
		if freed == 0 && !w.st.ReclaimPending() {
			break
		}
	}
	w.retryFull()

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
	// rare in CI. Process every posted completion each pass to a fixpoint so a
	// loaded runner cannot exit with flips still unapplied and no segment emptied.
	for pass := 0; pass < 2000 && s.ColdDraining(); pass++ {
		w.drainCold()
		for w.io.pool.out > 0 && w.advanceIntents() > 0 {
		}
	}
	w.io.stop()
	for w.io.pool.out > 0 {
		w.advanceIntents()
	}

	if s.ColdDraining() {
		t.Skip("shard still draining after the drain loop; window not reached on this run")
	}
	if !s.ReclaimPending() {
		// The drain emptied no whole segment on this interleaving, so the
		// drained queue is empty and the window this test isolates does not
		// exist to exercise. Skip rather than assert on a precondition the drain
		// did not construct, exactly as the still-draining case above skips; the
		// full drain-to-serve path is covered by TestBackpressureNoDropUnderDrain.
		t.Skip("drain emptied no whole segment on this run; reclaim-pending window not constructed")
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
