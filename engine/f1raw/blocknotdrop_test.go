package f1raw

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBackpressureConcurrentServesBeyondArena is the doc 23 block-not-drop gate under contention,
// the regime that broke the fixed-budget wait: many writers overrun a bounded arena at once, all
// blocking in waitForSegment while the migrator competes with them for the shard mutex it needs to
// flip drained index entries. Under the old fixed poll budget the migrator could not free a segment
// inside one second under this contention, the budget elapsed, and the blocked writes were dropped
// with ErrFull (the SET larger-than-memory SADD collapse). With the progress-gated wait a write
// blocks as long as the cold tail keeps advancing, so every write lands however slow the drain is.
//
// The test drives several arenas' worth of distinct records through a small segmented arena from
// many goroutines with the migrator engaged, and asserts no Set ever returns an error and every
// distinct key reads back its exact value afterward. A single dropped write shows up as a missing
// key on readback, so the readback is the no-truncation proof.
func TestBackpressureConcurrentServesBeyondArena(t *testing.T) {
	s := churnSegColdStore(t, 6)
	s.EnableMigrator()

	const writers = 16
	perSeg := int(s.segSize / align8(recSize(12, churnValLen)))
	total := perSeg * len(s.segs) * 4
	if total < writers {
		t.Fatalf("total %d smaller than writer count %d", total, writers)
	}

	var next int64
	var firstErr atomic.Value // error
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for {
				i := int(atomic.AddInt64(&next, 1) - 1)
				if i >= total {
					return
				}
				k := []byte(fmt.Sprintf("k%08d", i))
				if err := s.Set(k, churnVal("k", i)); err != nil {
					// A non-nil error here is the backpressure giving up and dropping the write, the
					// exact regression. Record the first and stop this writer.
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		})
	}
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		t.Fatalf("a concurrent Set was dropped with %v; block-not-drop backpressure did not hold under contention", err)
	}
	// Every distinct key must read back its exact value. A dropped write is a missing key here.
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal("k", i)) {
			t.Fatalf("key %q = %q,%v; want its exact value (i=%d/%d): a write was dropped", k, v, ok, i, total)
		}
	}
	// The migrator did real work: many writes blocked and none stalled, so waits climbed and stalls
	// stayed at zero. This is the healthy-overflow signature doc 23 D23-4 exposes in INFO.
	waits, stalls := s.BackpressureStats()
	if waits == 0 {
		t.Fatal("no backpressure waits recorded; the arena was never actually overrun, so the test proved nothing")
	}
	if stalls != 0 {
		t.Fatalf("backpressure stalled %d times on a workload the migrator can serve; the progress gate gave up early", stalls)
	}
}

// TestBackpressureServesBelowHighWater is the regression for the below-high-water livelock: a writer
// blocked in waitForSegment must free a segment through the migrator even when the resident live-byte
// total sits under the migrate-down high-water mark. Before the fix the migrator's only drain trigger
// was that high-water mark, so with it set above what the resident set ever reaches, migrateDown
// declined to drain and parked idle while the blocked writer's progress gate kept waiting on a drain
// target that never got drained, hanging forever. It drives one serial writer (no contention, so the
// hang is not a scheduling artifact) through several arenas' worth of records with the high-water mark
// pinned at 99% so the resident set, kept small by migration, never crosses it, and asserts every write
// lands. A tight deadline turns a reintroduced hang into a fast failure rather than a package timeout.
func TestBackpressureServesBelowHighWater(t *testing.T) {
	s := churnSegColdStore(t, 6)
	// Pin the steady-state drain trigger just below full: the resident set stays well under this once
	// migration is sinking records cold, so only the backpressure-forced drain can free a segment for a
	// blocked writer. loNum is one below so the tuning validates.
	s.SetMigratorTuning(99, 98, 0)
	s.EnableMigrator()

	perSeg := int(s.segSize / align8(recSize(12, churnValLen)))
	total := perSeg * len(s.segs) * 4

	done := make(chan error, 1)
	go func() {
		for i := 0; i < total; i++ {
			k := []byte(fmt.Sprintf("k%08d", i))
			if err := s.Set(k, churnVal("k", i)); err != nil {
				done <- fmt.Errorf("Set %q (i=%d/%d): %w", k, i, total, err)
				return
			}
		}
		done <- nil
	}()

	// The whole run is a few dozen 8MB drains; 60 seconds is generous headroom on a slow CI box while
	// still catching a genuine hang, which would otherwise consume the package timeout.
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("writes did not complete in 60s with the high-water mark above the resident set; the migrator did not drain for a blocked writer (below-high-water livelock)")
	}

	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal("k", i)) {
			t.Fatalf("key %q = %q,%v; want its exact value (i=%d/%d)", k, v, ok, i, total)
		}
	}
	if waits, _ := s.BackpressureStats(); waits == 0 {
		t.Fatal("no backpressure waits recorded; the arena was never overrun, so the below-high-water path was not exercised")
	}
}

// TestBackpressureStallSurfacesFull is the doc 23 liveness backstop: the progress-gated wait must
// give up with ErrFull when the migrator genuinely cannot make room, not block forever. It builds
// the no-migratable-residue stall from the taxonomy (D23-3): the arena is filled with collection-kind
// records, which the default migrator policy never sinks (only strings migrate), so every full
// segment holds only unretireable residue, pickDrainTarget finds nothing to drain, the cold tail
// never advances, and there is no way to free a segment. A write that finds the arena full must then
// report ErrFull within the bounded no-progress window rather than hang.
//
// The erroring write runs in a goroutine guarded by a timer so a regression that blocks forever
// fails by this assertion rather than by hanging the whole package test.
func TestBackpressureStallSurfacesFull(t *testing.T) {
	s := churnSegColdStore(t, 4)
	s.EnableMigrator()
	const collKind = byte(1) // no migratable-kind policy is set, so this kind never drains

	// Fill the arena with non-migratable collection records until a write reports the arena full.
	// Each PutKind that finds room lands a record; the one that finds none engages waitForSegment,
	// which, with nothing migratable to drain, stalls and reports ErrFull.
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			k := []byte(fmt.Sprintf("c%08d", i))
			if _, err := s.PutKind(k, churnVal("c", i), collKind); err != nil {
				done <- err
				return
			}
			if i > 1<<20 { // far more than the arena can hold: a full arena should have errored long ago
				done <- errors.New("filled over a million records without ErrFull; the arena never reported full")
				return
			}
		}
	}()

	// The stall budget is migStallWindow (about one second of no progress); allow generous slack, and
	// treat exceeding it as a hang, the failure the progress gate must not have.
	budget := migStallWindow + 10*time.Second
	select {
	case err := <-done:
		if !errors.Is(err, ErrFull) {
			t.Fatalf("filling a non-migratable arena returned %v; want ErrFull once the migrator genuinely stalls", err)
		}
	case <-time.After(budget):
		t.Fatalf("a write to a genuinely full arena did not return within %v; the progress gate blocked forever instead of surfacing ErrFull", budget)
	}

	// The give-up must be accounted as a stall, the genuinely-full signature INFO surfaces.
	if _, stalls := s.BackpressureStats(); stalls == 0 {
		t.Fatal("a write reported ErrFull but no backpressure stall was counted; the stall accounting is wrong")
	}
}
