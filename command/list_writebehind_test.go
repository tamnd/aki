package command

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// RPUSH runs through the deferred write-behind RMW path once the shard workers
// are started, the same staged-then-async route as INCR but for a list blob: the
// reply (the new length) is computed synchronously under the per-shard rmwLock
// and the durable B-tree write is fired asynchronously. The list stays in the
// listpack blob form throughout this test (total stays under
// list-max-listpack-size, 128), so every push takes the fast inline path and
// never falls back to the synchronous coll-form or promotion route.
//
// The no-lost-update witness mirrors the INCR one: N clients each pushing M
// elements onto one list must see the returned length walk exactly 1..N*M with
// no gaps and no duplicates, whatever the interleaving and whenever the async
// writes land. A missing rmwLock would let two pushers splice from the same
// current body and one element would be lost, showing up here as a duplicate
// length and a short final list. Running under -race also exercises the staged
// hot-cache hand-off between the connection goroutine and the shard worker.
func TestRPushWriteBehindNoLostUpdate(t *testing.T) {
	const (
		clients   = 8
		opsPerCli = 15 // 8*15 = 120 < 128, so the list never promotes
		total     = clients * opsPerCli
		key       = "wb-list"
	)

	d := newFuzzDispatcher(t)
	d.engine.StartWorker() // turn on the deferred write-behind path
	t.Cleanup(d.engine.StopWorker)

	lengths := make([][]int64, clients)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := networking.NewOfflineConn()
			got := make([]int64, 0, opsPerCli)
			for op := range opsPerCli {
				// Every element is unique so the final list contents are checkable.
				val := []byte(fmt.Sprintf("c%d-%d", id, op))
				argv := [][]byte{[]byte("RPUSH"), []byte(key), val}
				conn.ResetOut()
				d.Handle(conn, argv)
				v, _, err := resp.Decode(conn.OutBytes(), 0)
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if v.Type != resp.TypeInteger {
					t.Errorf("client %d: RPUSH replied type %c not integer", id, byte(v.Type))
					return
				}
				got = append(got, v.Integer)
			}
			lengths[id] = got
		}(c)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	all := make([]int64, 0, total)
	for _, r := range lengths {
		all = append(all, r...)
	}
	if len(all) != total {
		t.Fatalf("collected %d replies want %d", len(all), total)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i, v := range all {
		if v != int64(i+1) {
			t.Fatalf("write-behind RMW lost or duplicated a push at position %d: got %d want %d", i, v, i+1)
		}
	}

	// LLEN must observe the final length even before the async writes are all
	// flushed, because the read consults the staged hot-cache and pending writes
	// ahead of the B-tree.
	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("LLEN"), []byte(key)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final LLEN decode: %v", err)
	}
	if v.Type != resp.TypeInteger || v.Integer != total {
		t.Fatalf("final LLEN = %v want %d", v.Integer, total)
	}

	// Every unique element pushed must be present exactly once.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("LRANGE"), []byte(key), []byte("0"), []byte("-1")})
	arr, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final LRANGE decode: %v", err)
	}
	if arr.Type != resp.TypeArray {
		t.Fatalf("LRANGE replied type %c not array", byte(arr.Type))
	}
	if len(arr.Elems) != total {
		t.Fatalf("LRANGE returned %d elements want %d", len(arr.Elems), total)
	}
	seen := make(map[string]bool, total)
	for _, el := range arr.Elems {
		seen[string(el.Str)] = true
	}
	for id := range clients {
		for op := range opsPerCli {
			val := fmt.Sprintf("c%d-%d", id, op)
			if !seen[val] {
				t.Fatalf("element %q missing from final list", val)
			}
		}
	}
}
