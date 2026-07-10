package shard

import (
	"bytes"
	"fmt"
	"runtime"
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

	rt := New(shards, testArena, testSeg)
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
				if err := c.Do(OpEcho, nil, []byte(p)); err != nil {
					t.Error(err)
					return
				}
			case 1: // keyed write, spread over the keyspace
				k := fmt.Sprintf("k%02d", i%97)
				v := fmt.Sprintf("v%05d", i)
				lastVal[k] = v
				touched[rt.ShardOf([]byte(k))] = true
				want[i] = []byte("+OK\r\n")
				if err := c.Do(OpSet, []byte(k), []byte(v)); err != nil {
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
				if err := c.Do(OpGet, []byte(k), nil); err != nil {
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
