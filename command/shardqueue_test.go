package command

import (
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// TestShardQueueFIFO proves popAll returns a sequentially pushed run in
// first-in-first-out order. The Treiber stack is last-in-first-out by nature;
// the reverse in popAll is what restores publish order, which coalesceSets
// relies on to keep seeing the same batch shape it always has.
func TestShardQueueFIFO(t *testing.T) {
	var q shardQueue
	q.init()

	const n = 1000
	for i := 0; i < n; i++ {
		req := &writeReq{index: i}
		q.push(req)
	}
	got := q.popAll()
	for i := 0; i < n; i++ {
		if got == nil {
			t.Fatalf("popAll ran out at %d, want %d items", i, n)
		}
		if got.index != i {
			t.Fatalf("item %d has index %d, want %d (order not preserved)", i, got.index, i)
		}
		got = got.next.Load()
	}
	if got != nil {
		t.Fatal("popAll returned more items than were pushed")
	}
	if d := q.length.Load(); d != 0 {
		t.Fatalf("length = %d after draining, want 0", d)
	}
}

// TestShardQueueConcurrentNoLoss fires many producers at one queue and a single
// consumer draining it, the exact many-producers-one-consumer shape the hand-off
// runs under, and asserts every pushed request is delivered exactly once. Run
// under -race this exercises the lock-free push against the concurrent popAll.
func TestShardQueueConcurrentNoLoss(t *testing.T) {
	var q shardQueue
	q.init()

	const (
		producers   = 16
		perProducer = 5000
		total       = producers * perProducer
	)

	seen := make([]int32, total)
	var consumed int
	consumerDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			for head := q.popAll(); head != nil; head = head.next.Load() {
				seen[head.index]++
				consumed++
			}
			if consumed >= total {
				return
			}
			select {
			case <-stop:
				// Final sweep after producers signalled completion.
				for head := q.popAll(); head != nil; head = head.next.Load() {
					seen[head.index]++
					consumed++
				}
				return
			default:
			}
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				q.push(&writeReq{index: base + i})
			}
		}(p * perProducer)
	}
	wg.Wait()
	close(stop)
	<-consumerDone

	if consumed != total {
		t.Fatalf("consumed %d requests, want %d", consumed, total)
	}
	for i, c := range seen {
		if c != 1 {
			t.Fatalf("request %d delivered %d times, want exactly 1", i, c)
		}
	}
}

// shardZeroKeys returns n distinct keys that all hash to shard 0, so a stress
// test can concentrate every producer on one shard worker, which is where the
// lock-free hand-off has to be correct under the most contention.
func shardZeroKeys(prefix string, n int) [][]byte {
	keys := make([][]byte, 0, n)
	for i := 0; len(keys) < n; i++ {
		k := []byte(fmt.Sprintf("%s-%d", prefix, i))
		if keyspace.ShardOf(k) == 0 {
			keys = append(keys, k)
		}
	}
	return keys
}

// TestShardQueueStressNoDroppedWrite drives the full engine path: many clients
// fire blind SETs to distinct keys and INCRs to one shared counter, all hashing
// to a single shard so every write funnels through one queue and one worker.
// After a flush every SET must be present with its value and the counter must
// equal the number of INCRs, so neither the lock-free hand-off nor the RMW
// serialization may drop an acknowledged write. Under -race this is the
// many-producers-one-consumer interleaving the design turns on.
func TestShardQueueStressNoDroppedWrite(t *testing.T) {
	const (
		clients     = 12
		setsPerCli  = 200
		incrPerCli  = 200
		uniqueKeys  = clients * setsPerCli
		counterName = "ctr"
	)

	d := newFuzzDispatcher(t)
	d.engine.StartWorker()
	t.Cleanup(d.engine.StopWorker)

	// Keep the counter on shard 0 too so it shares the worker under test.
	counter := shardZeroKeys(counterName, 1)[0]
	keys := shardZeroKeys("wb", uniqueKeys)

	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := networking.NewOfflineConn()
			for op := range setsPerCli {
				k := keys[id*setsPerCli+op]
				val := []byte(fmt.Sprintf("v-%d-%d", id, op))
				conn.ResetOut()
				d.Handle(conn, [][]byte{[]byte("SET"), k, val})
			}
			for range incrPerCli {
				conn.ResetOut()
				d.Handle(conn, [][]byte{[]byte("INCR"), counter})
			}
		}(c)
	}
	wg.Wait()

	// Drain the shard queues so every acknowledged async write has reached the
	// B-tree before the assertions read back.
	d.engine.FlushShardWrites()

	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("GET"), counter})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("GET counter decode: %v", err)
	}
	wantCtr := fmt.Sprintf("%d", clients*incrPerCli)
	if string(v.Str) != wantCtr {
		t.Fatalf("counter = %q want %q (a lost RMW update through the queue)", v.Str, wantCtr)
	}

	for id := range clients {
		for op := range setsPerCli {
			k := keys[id*setsPerCli+op]
			want := fmt.Sprintf("v-%d-%d", id, op)
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("GET"), k})
			gv, _, derr := resp.Decode(conn.OutBytes(), 0)
			if derr != nil {
				t.Fatalf("GET %s decode: %v", k, derr)
			}
			if string(gv.Str) != want {
				t.Fatalf("GET %s = %q want %q (a SET was dropped by the hand-off)", k, gv.Str, want)
			}
		}
	}
}
