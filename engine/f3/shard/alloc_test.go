package shard

import (
	"fmt"
	"testing"
)

// TestDrainedPathZeroAllocs pins the F7 discipline on the drained execute
// path: once the free list, the reply buffers, and the worker's scratch are
// warm, a full cycle of enqueue, hop, prefetch, epoch-bracketed execute, and
// in-order reply emit allocates nothing. The runtime is not started; the test
// goroutine is the shard's owner and calls the drain directly, so the
// assertion measures exactly the path a worker runs per batch.
func TestDrainedPathZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation accounting is not meaningful under the race detector")
	}
	rt := testRuntime(1)
	c := rt.NewConn()
	w := rt.workers[0]

	val := []byte("value-of-64-bytes-0123456789012345678901234567890123456789012")
	setArgs := make([][][]byte, 16)
	getArgs := make([][][]byte, 16)
	for i := range setArgs {
		key := []byte(fmt.Sprintf("key%03d", i))
		setArgs[i] = [][]byte{key, val}
		getArgs[i] = [][]byte{key}
	}

	sink := make([]byte, 0, 64<<10)
	emit := func(rep []byte) { sink = append(sink, rep...) }

	run := func() {
		for i := range setArgs {
			if i%2 == 0 {
				if err := c.Do(opSet, true, setArgs[i]); err != nil {
					t.Error(err)
				}
			} else {
				if err := c.Do(opGet, true, getArgs[i]); err != nil {
					t.Error(err)
				}
			}
		}
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		sink = sink[:0]
		c.DrainReplies(emit)
	}

	// Warm: first inserts grow the index and the scratch buffers.
	for i := 0; i < 4; i++ {
		run()
	}
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("drained path allocates %.1f allocs/op, want 0", allocs)
	}
	if w.ep.owner.Load() != 0 {
		t.Fatal("worker left its epoch bracket open after the drain")
	}
}

// BenchmarkDrainExecute prices one enqueue-hop-execute-reply cycle of a full
// batch on the owner path, allocs reported so a regression shows in CI output.
func BenchmarkDrainExecute(b *testing.B) {
	rt := New(1, 64<<20, 0)
	rt.Use(testHandlers())
	c := rt.NewConn()
	w := rt.workers[0]

	val := []byte("value-of-64-bytes-0123456789012345678901234567890123456789012")
	setArgs := make([][][]byte, batchCap)
	getArgs := make([][][]byte, batchCap)
	for i := range setArgs {
		key := []byte(fmt.Sprintf("key%03d", i))
		setArgs[i] = [][]byte{key, val}
		getArgs[i] = [][]byte{key}
	}
	emit := func([]byte) {}

	run := func() {
		for i := range setArgs {
			if i%2 == 0 {
				if err := c.Do(opSet, true, setArgs[i]); err != nil {
					b.Fatal(err)
				}
			} else {
				if err := c.Do(opGet, true, getArgs[i]); err != nil {
					b.Fatal(err)
				}
			}
		}
		c.Flush()
		for w.drainAndExecute() > 0 {
		}
		c.DrainReplies(emit)
	}
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run()
	}
}
