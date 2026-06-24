package command

import (
	"sort"
	"sync"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// INCR runs through the deferred write-behind RMW path once the shard workers
// are started: the reply is computed synchronously under the per-shard rmwLock
// and the durable B-tree write is fired asynchronously. The no-lost-update
// witness from doc 23 section 8.8 must still hold on that path: N clients each
// running INCR M times against one counter must observe exactly 1..N*M with no
// gaps and no duplicates, whatever the interleaving and whenever the async
// writes land. A missing rmwLock would let two RMW readers see the same current
// value and one increment would be lost, showing up here as a duplicate reply
// and a missing value. Running under -race also exercises the staged hot-cache
// hand-off between the connection goroutine and the shard worker.
func TestINCRWriteBehindNoLostUpdate(t *testing.T) {
	const (
		clients   = 8
		opsPerCli = 500
		total     = clients * opsPerCli
		counter   = "wb-counter"
	)

	d := newFuzzDispatcher(t)
	d.engine.StartWorker() // turn on the deferred write-behind path
	t.Cleanup(d.engine.StopWorker)

	results := make([][]int64, clients)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := networking.NewOfflineConn()
			argv := [][]byte{[]byte("INCR"), []byte(counter)}
			got := make([]int64, 0, opsPerCli)
			for range opsPerCli {
				conn.ResetOut()
				d.Handle(conn, argv)
				v, _, err := resp.Decode(conn.OutBytes(), 0)
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if v.Type != resp.TypeInteger {
					t.Errorf("client %d: INCR replied type %c not integer", id, byte(v.Type))
					return
				}
				got = append(got, v.Integer)
			}
			results[id] = got
		}(c)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	all := make([]int64, 0, total)
	for _, r := range results {
		all = append(all, r...)
	}
	if len(all) != total {
		t.Fatalf("collected %d replies want %d", len(all), total)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i, v := range all {
		if v != int64(i+1) {
			t.Fatalf("write-behind RMW lost or duplicated an update at position %d: got %d want %d", i, v, i+1)
		}
	}

	// A read must observe the final value even before the async writes are all
	// flushed, because Get consults the staged hot-cache and pending writes
	// ahead of the B-tree.
	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("GET"), []byte(counter)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final GET decode: %v", err)
	}
	if string(v.Str) != "4000" {
		t.Fatalf("final counter = %q want %q", v.Str, "4000")
	}
}
