package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// HSET runs through the deferred write-behind RMW path once the shard workers
// are started: the reply (the count of newly added fields) is computed
// synchronously under the per-shard rmwLock and the durable B-tree write is
// fired asynchronously. The hash stays in the listpack blob form throughout this
// test (total stays under hash-max-listpack-entries, 128), so every HSET takes
// the fast inline path and never falls back to the synchronous coll-form or
// promotion route.
//
// The no-lost-update witness from doc 23 mirrors the INCR and RPUSH ones. N
// clients each set M distinct fields on one hash, so every HSET adds exactly one
// new field. The added counts must sum to N*M, the final HLEN must equal N*M, and
// every field must read back with its written value. A missing rmwLock would let
// two writers decode the same current body and one writer's staged blob would
// clobber the other's added field, showing up as a short final hash. Running
// under -race exercises the staged hot-cache hand-off between the connection
// goroutine and the shard worker.
func TestHSetWriteBehindNoLostUpdate(t *testing.T) {
	const (
		clients   = 8
		opsPerCli = 15 // 8*15 = 120 < 128, so the hash never promotes
		total     = clients * opsPerCli
		key       = "wb-hash"
	)

	d := newFuzzDispatcher(t)
	d.engine.StartWorker() // turn on the deferred write-behind path
	t.Cleanup(d.engine.StopWorker)

	added := make([]int64, clients)
	done := make(chan struct{}, clients)
	for c := range clients {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			conn := networking.NewOfflineConn()
			var sum int64
			for op := range opsPerCli {
				field := []byte(fmt.Sprintf("c%d-f%d", id, op))
				val := []byte(fmt.Sprintf("c%d-v%d", id, op))
				conn.ResetOut()
				d.Handle(conn, [][]byte{[]byte("HSET"), []byte(key), field, val})
				v, _, err := resp.Decode(conn.OutBytes(), 0)
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if v.Type != resp.TypeInteger {
					t.Errorf("client %d: HSET replied type %c not integer", id, byte(v.Type))
					return
				}
				sum += v.Integer
			}
			added[id] = sum
		}(c)
	}
	for range clients {
		<-done
	}
	if t.Failed() {
		return
	}

	var totalAdded int64
	for _, a := range added {
		totalAdded += a
	}
	if totalAdded != total {
		t.Fatalf("HSET added %d fields want %d (a lost update would drop one)", totalAdded, total)
	}

	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HLEN"), []byte(key)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final HLEN decode: %v", err)
	}
	if v.Type != resp.TypeInteger || v.Integer != total {
		t.Fatalf("final HLEN = %v want %d", v.Integer, total)
	}

	// Every field must read back with the value its writer set.
	for id := range clients {
		for op := range opsPerCli {
			field := fmt.Sprintf("c%d-f%d", id, op)
			want := fmt.Sprintf("c%d-v%d", id, op)
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("HGET"), []byte(key), []byte(field)})
			got, _, err := resp.Decode(conn.OutBytes(), 0)
			if err != nil {
				t.Fatalf("HGET %s decode: %v", field, err)
			}
			if string(got.Str) != want {
				t.Fatalf("HGET %s = %q want %q", field, got.Str, want)
			}
		}
	}
}
