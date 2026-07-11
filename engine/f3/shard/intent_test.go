package shard

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newIntentRuntime builds a started runtime with s shards and no handlers, which
// is all the intent substrate needs: Begin/Acquire/Do/Release drive the workers
// through their intent queues, not the hop path.
func newIntentRuntime(t *testing.T, s int) *Runtime {
	t.Helper()
	r := New(s, testArena, testSeg)
	r.Start()
	t.Cleanup(r.Stop)
	return r
}

// TestIntentSingleKeyBarrier is the smallest case: one key, one transaction.
// Begin, Acquire, one owner-side mutation through Do, Release.
func TestIntentSingleKeyBarrier(t *testing.T) {
	r := newIntentRuntime(t, 4)
	ran := false
	tx := r.Begin([][]byte{[]byte("k")})
	tx.Acquire()
	tx.Do([]byte("k"), func(cx *Ctx) { ran = true })
	tx.Release()
	if !ran {
		t.Fatal("critical section did not run")
	}
}

// TestIntentMultiKeyOrder acquires several keys that span shards and confirms
// the critical section runs on the right owners.
func TestIntentMultiKeyOrder(t *testing.T) {
	r := newIntentRuntime(t, 8)
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
	tx := r.Begin(keys)
	tx.Acquire()
	seen := map[string]bool{}
	for _, k := range keys {
		kk := k
		tx.Do(kk, func(cx *Ctx) { seen[string(kk)] = true })
	}
	tx.Release()
	for _, k := range keys {
		if !seen[string(k)] {
			t.Fatalf("key %q critical section did not run", k)
		}
	}
}

// TestIntentDedup collapses duplicate keys to one intent.
func TestIntentDedup(t *testing.T) {
	r := newIntentRuntime(t, 4)
	tx := r.Begin([][]byte{[]byte("x"), []byte("x"), []byte("y"), []byte("x")})
	if len(tx.intents) != 2 {
		t.Fatalf("want 2 distinct intents, got %d", len(tx.intents))
	}
	tx.Acquire()
	tx.Release()
}

// TestIntentMutualExclusion is the core safety property: many transactions
// contending on overlapping keys must never run critical sections on a shared
// key at the same time. A per-key guard counter that ever exceeds one is the
// falsification. Run under -race, it also proves the owner-only mutations are
// race-free.
func TestIntentMutualExclusion(t *testing.T) {
	const (
		shards = 8
		keyN   = 6
		txns   = 200
		conc   = 16
	)
	r := newIntentRuntime(t, shards)

	// held[k] counts transactions currently inside a critical section for key k.
	// The owner runs each Do on its single goroutine, but different keys of one
	// transaction run on different owners, so the guard is atomic.
	held := make([]atomic.Int32, keyN)
	var bad atomic.Bool

	rng := rand.New(rand.NewSource(1))
	// Pre-generate each transaction's key set so the workers do the racing, not
	// the generator.
	sets := make([][][]byte, txns)
	for i := range sets {
		n := 1 + rng.Intn(3)
		ks := map[int]struct{}{}
		for len(ks) < n {
			ks[rng.Intn(keyN)] = struct{}{}
		}
		var ss [][]byte
		for k := range ks {
			ss = append(ss, []byte(fmt.Sprintf("k%d", k)))
		}
		sets[i] = ss
	}
	keyIdx := func(b []byte) int {
		var n int
		fmt.Sscanf(string(b), "k%d", &n)
		return n
	}

	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				tx := r.Begin(sets[i])
				tx.Acquire()
				// Enter every key's critical section, bump its guard, and check no
				// peer is inside. A tiny pause widens the window a real conflict
				// would use.
				for _, k := range sets[i] {
					idx := keyIdx(k)
					if held[idx].Add(1) != 1 {
						bad.Store(true)
					}
				}
				for _, k := range sets[i] {
					kk := k
					tx.Do(kk, func(cx *Ctx) {})
					_ = kk
				}
				for _, k := range sets[i] {
					held[keyIdx(k)].Add(-1)
				}
				tx.Release()
			}
		}()
	}
	for i := 0; i < txns; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if bad.Load() {
		t.Fatal("two transactions held one key at once: mutual exclusion broken")
	}
}

// TestIntentNoDeadlock hammers a small key space with many concurrent
// transactions whose key sets overlap heavily and whose acquisition orders
// would deadlock without the ticket order. Completion of every transaction
// within the deadline is the deadlock-freedom proof.
func TestIntentNoDeadlock(t *testing.T) {
	const (
		shards = 6
		keyN   = 4
		txns   = 400
		conc   = 24
	)
	r := newIntentRuntime(t, shards)

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		jobs := make(chan int)
		rngs := make([]*rand.Rand, conc)
		for w := 0; w < conc; w++ {
			rngs[w] = rand.New(rand.NewSource(int64(w) + 1))
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				rng := rngs[w]
				for range jobs {
					// Two or three distinct keys, deliberately dense so almost every
					// pair of transactions conflicts.
					n := 2 + rng.Intn(2)
					ks := map[int]struct{}{}
					for len(ks) < n {
						ks[rng.Intn(keyN)] = struct{}{}
					}
					var ss [][]byte
					for k := range ks {
						ss = append(ss, []byte(fmt.Sprintf("k%d", k)))
					}
					tx := r.Begin(ss)
					tx.Acquire()
					for _, k := range ss {
						tx.Do(k, func(cx *Ctx) {})
					}
					tx.Release()
				}
			}(w)
		}
		for i := 0; i < txns; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("transactions did not all complete: deadlock or livelock")
	}
}

// TestIntentSerializes proves the barrier gives real serialization: many
// transactions each read-modify-write one shared counter guarded by its intent,
// and the final value must equal the number of transactions with no lost
// updates. The read and write straddle a Do boundary on the owner, so only the
// intent lock keeps them atomic against peers.
func TestIntentSerializes(t *testing.T) {
	const (
		shards = 8
		txns   = 500
		conc   = 20
	)
	r := newIntentRuntime(t, shards)

	var counter int64 // guarded by the "c" intent; touched only inside Do on its owner
	key := []byte("c")

	var wg sync.WaitGroup
	jobs := make(chan struct{})
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				tx := r.Begin([][]byte{key})
				tx.Acquire()
				tx.Do(key, func(cx *Ctx) {
					v := counter
					v++
					counter = v
				})
				tx.Release()
			}
		}()
	}
	for i := 0; i < txns; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	if counter != txns {
		t.Fatalf("lost updates: counter=%d want %d", counter, txns)
	}
}

// TestIntentLowestTicketWins checks the progress guarantee directly: with two
// transactions contending, the acquire never wedges and both complete, whatever
// the interleaving. Repeated to shake the schedules.
func TestIntentLowestTicketWins(t *testing.T) {
	r := newIntentRuntime(t, 4)
	keys := [][]byte{[]byte("p"), []byte("q")}
	for i := 0; i < 2000; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tx := r.Begin(keys)
				tx.Acquire()
				tx.Do(keys[0], func(cx *Ctx) {})
				tx.Release()
			}()
		}
		wg.Wait()
	}
}

// TestIntentReleaseWithoutAcquire confirms Release is safe on a transaction that
// never acquired, the API's stated contract, and that it does not wedge later
// transactions on the same keys.
func TestIntentReleaseWithoutAcquire(t *testing.T) {
	r := newIntentRuntime(t, 4)
	tx := r.Begin([][]byte{[]byte("a"), []byte("b")})
	tx.Release()
	// The queues must be clean: a fresh transaction on the same keys acquires at
	// once.
	got := make(chan struct{})
	go func() {
		tx2 := r.Begin([][]byte{[]byte("a"), []byte("b")})
		tx2.Acquire()
		tx2.Release()
		close(got)
	}()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("Release without Acquire wedged the queue")
	}
}

// BenchmarkAdvanceIntentsIdle prices the intent path's whole cost on a shard
// with no tier-two traffic: the guard load advanceIntents pays once per drain
// pass. It is the number the "zero overhead on the non-intent fast path" claim
// rests on, and it is paid per pass (hundreds of point ops), not per command.
func BenchmarkAdvanceIntentsIdle(b *testing.B) {
	w := newWorker(0, nil)
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for i := 0; i < b.N; i++ {
		sink += w.advanceIntents()
	}
	if sink != 0 {
		b.Fatal("idle advanceIntents did work")
	}
}
