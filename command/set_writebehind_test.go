package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// SADD runs through the deferred write-behind RMW path once the shard workers are
// started: the reply (the count of newly added members) is computed synchronously
// under the per-shard rmwLock and the durable B-tree write is fired
// asynchronously. The set stays in the listpack blob form throughout this test
// (total stays under set-max-listpack-entries, 128, and the members are
// non-integer so the intset form never applies), so every SADD takes the fast
// inline path and never falls back to the synchronous coll-form or promotion
// route.
//
// The no-lost-update witness mirrors the HSET one. N clients each add M distinct
// members to one set, so every SADD adds exactly one new member. The added counts
// must sum to N*M, the final SCARD must equal N*M, and every member must read back
// as present. A missing rmwLock would let two writers decode the same current body
// and one writer's staged blob would clobber the other's added member, showing up
// as a short final set. Running under -race exercises the staged hot-cache
// hand-off between the connection goroutine and the shard worker.
func TestSAddWriteBehindNoLostUpdate(t *testing.T) {
	const (
		clients   = 8
		opsPerCli = 15 // 8*15 = 120 < 128, so the set never promotes
		total     = clients * opsPerCli
		key       = "wb-set"
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
				member := []byte(fmt.Sprintf("c%d-m%d", id, op))
				conn.ResetOut()
				d.Handle(conn, [][]byte{[]byte("SADD"), []byte(key), member})
				v, _, err := resp.Decode(conn.OutBytes(), 0)
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if v.Type != resp.TypeInteger {
					t.Errorf("client %d: SADD replied type %c not integer", id, byte(v.Type))
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
		t.Fatalf("SADD added %d members want %d (a lost update would drop one)", totalAdded, total)
	}

	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("SCARD"), []byte(key)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final SCARD decode: %v", err)
	}
	if v.Type != resp.TypeInteger || v.Integer != total {
		t.Fatalf("final SCARD = %v want %d", v.Integer, total)
	}

	// Every member must read back as present.
	for id := range clients {
		for op := range opsPerCli {
			member := fmt.Sprintf("c%d-m%d", id, op)
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("SISMEMBER"), []byte(key), []byte(member)})
			got, _, err := resp.Decode(conn.OutBytes(), 0)
			if err != nil {
				t.Fatalf("SISMEMBER %s decode: %v", member, err)
			}
			if got.Type != resp.TypeInteger || got.Integer != 1 {
				t.Fatalf("SISMEMBER %s = %v want 1", member, got.Integer)
			}
		}
	}
}
