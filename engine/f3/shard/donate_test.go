package shard

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The worker-donation suite (donate.go): FanOut runs every task exactly once,
// inline without a runtime, degraded when the pool is busy, in parallel when
// the pool is idle, under concurrent coordinators without deadlock, and inside
// an intent critical section. The -race build is the memory-model check: task
// writes must be visible to the coordinator after FanOut returns with no
// synchronization beyond the job's counters.

// gid returns the calling goroutine's id, parsed from the stack header. Test
// only: it exists to prove donated tasks really ran on donee goroutines.
func gid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := buf[:n]
	s = s[len("goroutine "):]
	i := bytes.IndexByte(s, ' ')
	id, _ := strconv.ParseUint(string(s[:i]), 10, 64)
	return id
}

// spin burns roughly d of CPU without blocking, the stand-in for a merge
// task's work. Sleeping would park the goroutine, which a donated task must
// never do.
func spin(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
	}
}

// keyOnShard returns a key that routes to the given shard.
func keyOnShard(t *testing.T, rt *Runtime, shard int) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		k := fmt.Sprintf("k%d", i)
		if rt.ShardOf([]byte(k)) == shard {
			return k
		}
	}
	t.Fatalf("no key found for shard %d", shard)
	return ""
}

// waitPoolIdle blocks until every worker in the pool has reached stateParked,
// the settled-idle precondition a donation test needs before it can assert that
// FanOut engages: only then are the donees observably idle at offer time. Once
// parked with no traffic a worker stays parked (nothing wakes it), so the state
// is stable once observed. It fails rather than hang if the pool never settles.
func waitPoolIdle(t *testing.T, rt *Runtime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allParked := true
		for _, w := range rt.workers {
			if w.wk.state.Load() != stateParked {
				allParked = false
				break
			}
		}
		if allParked {
			return
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Fatal("pool never settled to idle")
}

func TestFanOutInlineBareCtx(t *testing.T) {
	var cx Ctx
	ran := make([]int, 16)
	cx.FanOut(len(ran), func(k int) { ran[k]++ })
	for k, n := range ran {
		if n != 1 {
			t.Fatalf("task %d ran %d times, want 1", k, n)
		}
	}
	cx.FanOut(0, func(k int) { t.Fatal("n=0 must run nothing") })
	one := 0
	cx.FanOut(1, func(k int) { one++ })
	if one != 1 {
		t.Fatalf("n=1 ran %d times", one)
	}
}

// fanRuntime builds a started runtime whose opFan handler fans out n tasks
// through the shard's real Ctx and replies with a status.
func fanRuntime(t *testing.T, shards int, task func(k int), n int) *Runtime {
	t.Helper()
	h := testHandlers()
	h = append(h, func(cx *Ctx, args [][]byte, r Reply) {
		cx.FanOut(n, task)
		r.Status("DONE")
	})
	rt := New(shards, testArena, testSeg)
	rt.Use(h)
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// opFan is the index the fan handler lands at when appended to testHandlers.
const opFan = opIncr + 1

func TestFanOutAllTasksOnce(t *testing.T) {
	const tasks = 64
	var counts [tasks]atomic.Int32
	rt := fanRuntime(t, 8, func(k int) { counts[k].Add(1) }, tasks)
	c := rt.NewConn()
	if err := c.Do(opFan, true, args("k")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	rep := collect(t, c, 1)
	if string(rep[0]) != "+DONE\r\n" {
		t.Fatalf("reply = %q", rep[0])
	}
	for k := range counts {
		if n := counts[k].Load(); n != 1 {
			t.Fatalf("task %d ran %d times, want exactly 1", k, n)
		}
	}
}

// TestFanOutUsesDonees proves the parallelism is real: with an idle pool, more
// than one goroutine must execute tasks concurrently. Rather than race the
// donees' wake against a serial drain (a timing bet that loses on a loaded
// runner, where the coordinator finishes every spin-task before any donee is
// scheduled), each task parks on a shared gate until two distinct goroutines
// are simultaneously inside tasks. The coordinator holding one task cannot
// drain the rest, since FanOut offers the whole job to idle donees up front, so
// the donees claim the remaining tasks off the cursor: the moment a second
// goroutine enters, overlap is witnessed and the gate opens. This can only fail
// if donation never engages at all, never because a runner was slow: a late
// donee wake just makes the witness arrive later, and the coordinator is parked
// waiting for it, not draining past it. This is the live half of the k-way
// scaling story; the magnitude is the lab's to measure
// (labs/f3/m1/09_donation_live).
func TestFanOutUsesDonees(t *testing.T) {
	const tasks = 64
	const need = 2 // distinct goroutines that must be inside tasks at once
	var mu sync.Mutex
	seen := map[uint64]bool{}
	release := make(chan struct{})
	overlapped := make(chan struct{})
	var once sync.Once
	rt := fanRuntime(t, 8, func(k int) {
		mu.Lock()
		seen[gid()] = true
		n := len(seen)
		mu.Unlock()
		if n >= need {
			once.Do(func() { close(overlapped) })
		}
		<-release // hold the goroutine so the coordinator cannot drain serially
	}, tasks)
	c := rt.NewConn()
	// FanOut offers the job only to workers it observes idle, and a worker's
	// zero-value state is stateRunning until its goroutine first reaches the
	// idle park. On a CPU-starved runner the donee goroutines may not have been
	// scheduled that far when the command lands, so FanOut would see them all
	// running, offer to none, and correctly degrade to serial: the test would
	// then time out through no fault of the donation path. Wait for the pool to
	// settle into parked so the "idle donees" premise this test asserts on
	// actually holds before the command runs.
	waitPoolIdle(t, rt)
	if err := c.Do(opFan, true, args("k")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	select {
	case <-overlapped:
	case <-time.After(15 * time.Second):
		close(release)
		collect(t, c, 1)
		t.Fatal("donation never engaged: fewer than 2 goroutines ran tasks concurrently")
	}
	close(release)
	collect(t, c, 1)
	mu.Lock()
	got := len(seen)
	mu.Unlock()
	if got < need {
		t.Fatalf("only %d goroutines ran tasks; donation never engaged", got)
	}
}

// TestFanOutConcurrentCoordinators runs two fanning commands on two shards at
// once, repeatedly: the shared-cursor design must let both finish even when
// each coordinator's offers land on the other's busy worker. A deadlock here
// trips the collect deadline.
func TestFanOutConcurrentCoordinators(t *testing.T) {
	const tasks = 32
	const rounds = 50
	var total atomic.Int64
	rt := fanRuntime(t, 8, func(k int) {
		spin(20 * time.Microsecond)
		total.Add(1)
	}, tasks)
	k0 := keyOnShard(t, rt, 0)
	k1 := keyOnShard(t, rt, 1)
	var wg sync.WaitGroup
	for _, key := range []string{k0, k1} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			c := rt.NewConn()
			for i := 0; i < rounds; i++ {
				if err := c.Do(opFan, true, args(key)); err != nil {
					t.Error(err)
					return
				}
				c.Flush()
				collect(t, c, 1)
			}
		}(key)
	}
	wg.Wait()
	if got := total.Load(); got != 2*rounds*tasks {
		t.Fatalf("ran %d tasks, want %d", got, 2*rounds*tasks)
	}
}

// TestFanOutDoneesBusy saturates the only other worker with its own traffic
// while a command fans out: completion must never depend on a donee showing
// up, so the coordinator finishes the job itself.
func TestFanOutDoneesBusy(t *testing.T) {
	const tasks = 16
	var counts [tasks]atomic.Int32
	rt := fanRuntime(t, 2, func(k int) { counts[k].Add(1) }, tasks)
	k0 := keyOnShard(t, rt, 0)
	k1 := keyOnShard(t, rt, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := rt.NewConn()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := c.Do(opSet, true, args(k1, "v")); err != nil {
				return
			}
			c.Flush()
			c.DrainReplies(func([]byte) {})
		}
	}()

	c := rt.NewConn()
	for i := 0; i < 20; i++ {
		if err := c.Do(opFan, true, args(k0)); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		collect(t, c, 1)
	}
	close(stop)
	wg.Wait()
	for k := range counts {
		if n := counts[k].Load(); n != 20 {
			t.Fatalf("task %d ran %d times, want 20", k, n)
		}
	}
}

// TestFanOutInsideIntentBarrier fans out from inside a Txn critical section,
// the cross-shard form doc 11 section 6.5 describes: the intent barrier is the
// freeze and the donation is the parallelism. The owner running Do is the
// coordinator, so the donees are the rest of the pool.
func TestFanOutInsideIntentBarrier(t *testing.T) {
	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	const tasks = 32
	var counts [tasks]atomic.Int32
	key := []byte("intent-fan")
	txn := rt.Begin([][]byte{key})
	txn.Acquire()
	txn.Do(key, func(cx *Ctx) {
		cx.FanOut(tasks, func(k int) {
			spin(10 * time.Microsecond)
			counts[k].Add(1)
		})
	})
	txn.Release()
	for k := range counts {
		if n := counts[k].Load(); n != 1 {
			t.Fatalf("task %d ran %d times, want 1", k, n)
		}
	}
}
