package shard

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestCrossShardPipelineOrder is the order proof the hop transport owes (spec
// 2064/f3/milestones/M0.md): a deep pipeline whose commands route to
// different shards must come back in exact request order, even though the
// shards execute independently and complete in whatever order they like. The
// reader goroutine issues and flushes in small windows so batches from
// different shards are in flight together; the writer side asserts the emit
// order equals the issue order byte for byte.
func TestCrossShardPipelineOrder(t *testing.T) {
	const shards = 4
	const n = 5000

	rt := testRuntime(shards)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	// Build the expected reply for each request at issue time.
	want := make([][]byte, n)
	lastVal := make(map[string]string)

	touched := make(map[int]bool)
	issue := func() {
		for i := 0; i < n; i++ {
			switch i % 3 {
			case 0: // keyless, round-robins across shards
				p := fmt.Sprintf("e%05d", i)
				want[i] = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
				if err := c.Do(opEcho, false, args(p)); err != nil {
					t.Error(err)
					return
				}
			case 1: // keyed write, spread over the keyspace
				k := fmt.Sprintf("k%02d", i%97)
				v := fmt.Sprintf("v%05d", i)
				lastVal[k] = v
				touched[rt.ShardOf([]byte(k))] = true
				want[i] = []byte("+OK\r\n")
				if err := c.Do(opSet, true, args(k, v)); err != nil {
					t.Error(err)
					return
				}
			case 2: // keyed read of whatever this connection last wrote
				k := fmt.Sprintf("k%02d", (i-1)%97)
				v, ok := lastVal[k]
				if ok {
					want[i] = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
				} else {
					want[i] = []byte("$-1\r\n")
				}
				if err := c.Do(opGet, true, args(k)); err != nil {
					t.Error(err)
					return
				}
			}
			// Flush at a prime stride so per-shard batches interleave in
			// flight instead of arriving one shard at a time.
			if i%17 == 16 {
				c.Flush()
			}
		}
		c.Flush()
	}
	go issue()

	got := make([][]byte, 0, n)
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < n {
		c.DrainReplies(func(rep []byte) {
			got = append(got, append([]byte(nil), rep...))
		})
		if len(got) < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out with %d of %d replies", len(got), n)
			}
			runtime.Gosched()
		}
	}

	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("reply %d out of order or wrong: got %q, want %q", i, got[i], want[i])
		}
	}
	if len(touched) < 2 {
		t.Fatalf("keys landed on %d shard(s); the test needs a genuine cross-shard pipeline", len(touched))
	}
}

// TestPipelineWakeStress hammers the park/wake protocol from every side the
// wake-skip and per-pass coalescing rules touch: many connections, one or
// two commands per flush so nearly every push crosses the empty-to-non-empty
// edge, and random think time on the issuers so workers and writers park and
// get woken constantly instead of staying saturated. Each connection's
// drainer uses Wait, the parking writer path the transport runs. The
// assertion is liveness plus delivery: every connection gets every reply, in
// request order, before the watchdog fires. A lost wake shows up as the
// watchdog timeout. Run it under -race; the CI f3 race pass covers it.
func TestPipelineWakeStress(t *testing.T) {
	const shards = 4
	const conns = 24
	n := 800
	if testing.Short() {
		n = 200
	}

	rt := testRuntime(shards)
	rt.Start()
	defer rt.Stop()

	var wg sync.WaitGroup
	for cid := 0; cid < conns; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			c := rt.NewConn()
			rng := rand.New(rand.NewSource(int64(cid)*7919 + 1))

			want := make([][]byte, n)
			lastVal := make(map[string]string)
			go func() {
				for i := 0; i < n; i++ {
					switch i % 3 {
					case 0: // keyless, round-robins across shards
						p := fmt.Sprintf("c%02d-e%05d", cid, i)
						want[i] = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
						if err := c.Do(opEcho, false, args(p)); err != nil {
							t.Error(err)
							return
						}
					case 1: // keyed write in a per-connection keyspace
						k := fmt.Sprintf("c%02d-k%02d", cid, i%37)
						v := fmt.Sprintf("v%05d", i)
						lastVal[k] = v
						want[i] = []byte("+OK\r\n")
						if err := c.Do(opSet, true, args(k, v)); err != nil {
							t.Error(err)
							return
						}
					case 2: // keyed read of this connection's last write
						k := fmt.Sprintf("c%02d-k%02d", cid, (i-1)%37)
						if v, ok := lastVal[k]; ok {
							want[i] = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
						} else {
							want[i] = []byte("$-1\r\n")
						}
						if err := c.Do(opGet, true, args(k)); err != nil {
							t.Error(err)
							return
						}
					}
					// Tiny batches: flush after one or two commands so almost
					// every hop batch is its own push and wake decision.
					if rng.Intn(2) == 0 {
						c.Flush()
					}
					// Random think time so queues drain and both sides park.
					if rng.Intn(8) == 0 {
						time.Sleep(time.Duration(rng.Intn(40)) * time.Microsecond)
					}
				}
				c.Flush()
			}()

			// Drain to the exact count on the parking Wait path; Close is not
			// the completion signal because it drops in-flight replies by
			// contract. A lost wake leaves Wait parked and the watchdog fires.
			got := make([][]byte, 0, n)
			for len(got) < n && c.Wait() {
				c.DrainReplies(func(rep []byte) {
					got = append(got, append([]byte(nil), rep...))
				})
			}
			if len(got) != n {
				t.Errorf("conn %d: got %d of %d replies", cid, len(got), n)
				return
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Errorf("conn %d reply %d wrong or out of order: got %q, want %q", cid, i, got[i], want[i])
					return
				}
			}
		}(cid)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("stress run hung; a writer or worker missed a wake")
	}
}
