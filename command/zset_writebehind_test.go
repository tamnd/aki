package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// ZADD runs through the deferred write-behind RMW path once the shard workers are
// started: the reply (the count of newly added members) is computed synchronously
// under the per-shard rmwLock and the durable B-tree write is fired
// asynchronously. The sorted set stays in the listpack blob form throughout this
// test (total stays under zset-max-listpack-entries, 128), so every ZADD takes the
// fast inline path and never falls back to the synchronous coll-form or promotion
// route.
//
// The no-lost-update witness mirrors the HSET and SADD ones. N clients each add M
// distinct members to one sorted set, so every ZADD adds exactly one new member.
// The added counts must sum to N*M, the final ZCARD must equal N*M, and every
// member must read back with the score its writer set. A missing rmwLock would let
// two writers decode the same current body and one writer's staged blob would
// clobber the other's added member, showing up as a short final set. Running under
// the debug build and -race exercises the staged hot-cache hand-off between the
// connection goroutine and the shard worker.
func TestZAddWriteBehindNoLostUpdate(t *testing.T) {
	const (
		clients   = 8
		opsPerCli = 15 // 8*15 = 120 < 128, so the zset never promotes
		total     = clients * opsPerCli
		key       = "wb-zset"
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
				score := []byte(fmt.Sprintf("%d", id*opsPerCli+op))
				conn.ResetOut()
				d.Handle(conn, [][]byte{[]byte("ZADD"), []byte(key), score, member})
				v, _, err := resp.Decode(conn.OutBytes(), 0)
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if v.Type != resp.TypeInteger {
					t.Errorf("client %d: ZADD replied type %c not integer", id, byte(v.Type))
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
		t.Fatalf("ZADD added %d members want %d (a lost update would drop one)", totalAdded, total)
	}

	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("ZCARD"), []byte(key)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final ZCARD decode: %v", err)
	}
	if v.Type != resp.TypeInteger || v.Integer != total {
		t.Fatalf("final ZCARD = %v want %d", v.Integer, total)
	}

	// Every member must read back with the score its writer set.
	for id := range clients {
		for op := range opsPerCli {
			member := fmt.Sprintf("c%d-m%d", id, op)
			want := fmt.Sprintf("%d", id*opsPerCli+op)
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("ZSCORE"), []byte(key), []byte(member)})
			got, _, err := resp.Decode(conn.OutBytes(), 0)
			if err != nil {
				t.Fatalf("ZSCORE %s decode: %v", member, err)
			}
			if string(got.Str) != want {
				t.Fatalf("ZSCORE %s = %q want %q", member, got.Str, want)
			}
		}
	}
}
