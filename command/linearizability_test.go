package command

import (
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// This is the linearizability layer of doc 23 section 8.8. INCR on a single key
// has a clean linearizability witness: if N clients each run INCR M times against
// one counter, the replies must be exactly the integers 1..N*M with no gaps and no
// duplicates, whatever the interleaving. A lost update shows up as a missing value;
// a double-apply shows up as a duplicate. The engine serializes every write under
// one lock and commits each, so this holds, and running it under the race detector
// also checks the dispatcher is safe under concurrent connections.

func TestINCRLinearizable(t *testing.T) {
	const (
		clients     = 8
		opsPerCli   = 250
		total       = clients * opsPerCli
		counterName = "counter"
	)

	d := newFuzzDispatcher(t)

	results := make([][]int64, clients)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := networking.NewOfflineConn()
			argv := [][]byte{[]byte("INCR"), []byte(counterName)}
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
			t.Fatalf("linearizability violation at position %d: got %d want %d (duplicate or lost update)", i, v, i+1)
		}
	}

	// The final counter value must equal the number of increments.
	conn := networking.NewOfflineConn()
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("GET"), []byte(counterName)})
	v, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		t.Fatalf("final GET decode: %v", err)
	}
	if string(v.Str) != strconv.Itoa(total) {
		t.Fatalf("final counter = %q want %d", v.Str, total)
	}
}
