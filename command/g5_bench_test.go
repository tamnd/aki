package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// g5BenchTags are hash-tagged key prefixes for the G5 multi-core scaling
// benchmark. Each entry routes to a distinct keyspace shard, verified by
// TestG5BenchTagsDistinct. Matches the g5Tags in bench/dict_test.go.
var g5BenchTags = [keyspace.NumShards]string{
	"{g6}", // shard 0
	"{g7}", // shard 1
	"{g4}", // shard 2
	"{g5}", // shard 3
	"{g2}", // shard 4
	"{g3}", // shard 5
	"{g0}", // shard 6
	"{g1}", // shard 7
}

// TestG5BenchTagsDistinct verifies every g5BenchTags entry routes to a unique shard.
func TestG5BenchTagsDistinct(t *testing.T) {
	seen := make(map[int]string, keyspace.NumShards)
	for i, tag := range g5BenchTags {
		k := []byte(tag + ":key:00000000")
		s := keyspace.ShardOf(k)
		if prev, dup := seen[s]; dup {
			t.Fatalf("g5BenchTags[%d]=%q and %q both map to shard %d", i, tag, prev, s)
		}
		seen[s] = tag
	}
}

// startG5Server starts a full server (networking + write-behind engine) for
// benchmarking and returns its listen address. Workers are activated via
// StartBackground so the write-behind path (per-shard async channels) is live.
func startG5Server(b *testing.B) string {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "g5.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		b.Fatal(err)
	}
	ks, err := keyspace.Open(p)
	if err != nil {
		b.Fatal(err)
	}
	e := NewEngine(ks)
	d := New(Config{Engine: e})
	// StartBackground activates the per-shard write workers so SET commands
	// take the write-behind path and return before the B-tree write completes.
	d.StartBackground()
	ncfg := networking.Config{Addr: "127.0.0.1:0"}
	srv := networking.New(ncfg, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(ncfg) }()
	b.Cleanup(func() {
		_ = srv.Close()
		d.StopBackground()
		_ = p.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			b.Fatal("server did not bind within 5s")
		}
		time.Sleep(time.Millisecond)
	}
	return srv.Addr().String()
}

// dialG5 opens one TCP connection to addr with no read deadline (suitable for
// sustained benchmark loops that run longer than a fixed timeout).
func dialG5(b *testing.B, addr string) (net.Conn, *bufio.Reader) {
	b.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

// BenchmarkServerSetG5Sequential is the single-stream baseline for the G5
// comparison. One goroutine issues non-pipelined SET commands against one shard
// via the write-behind path.
func BenchmarkServerSetG5Sequential(b *testing.B) {
	addr := startG5Server(b)
	conn, br := dialG5(b, addr)
	defer conn.Close()
	tag := g5BenchTags[0]
	b.ResetTimer()
	i := 0
	for b.Loop() {
		key := fmt.Sprintf("%s:k:%08d", tag, i)
		cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key)
		if _, err := conn.Write([]byte(cmd)); err != nil {
			b.Fatal(err)
		}
		reply, err := br.ReadString('\n')
		if err != nil {
			b.Fatal(err)
		}
		if !strings.HasPrefix(reply, "+OK") {
			b.Fatalf("SET reply = %q want +OK", reply)
		}
		i++
	}
}

// BenchmarkServerSetG5Parallel is the G5 multi-core scaling benchmark (spec
// perf/00 §P2 / perf/01 §15). At GOMAXPROCS=1 one goroutine drives one shard.
// At GOMAXPROCS=8 eight goroutines each drive a distinct shard via hash-tagged
// keys so no two goroutines share a shard writer. The aggregate throughput at
// GOMAXPROCS=8 should be at least 4x the throughput at GOMAXPROCS=1.
//
// Run with: go test -bench=BenchmarkServerSetG5 -benchtime=5s -cpu=1,8
// The ns/op ratio (sequential / parallel) is the scaling factor; >= 4x passes G5.
func BenchmarkServerSetG5Parallel(b *testing.B) {
	addr := startG5Server(b)
	var mu sync.Mutex
	workerID := 0
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		myID := workerID % keyspace.NumShards
		workerID++
		mu.Unlock()

		tag := g5BenchTags[myID]
		conn, br := dialG5(b, addr)
		defer conn.Close()
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("%s:k:%08d", tag, i)
			cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key)
			if _, err := conn.Write([]byte(cmd)); err != nil {
				b.Error(err)
				return
			}
			reply, err := br.ReadString('\n')
			if err != nil {
				b.Error(err)
				return
			}
			if !strings.HasPrefix(reply, "+OK") {
				b.Errorf("SET reply = %q want +OK", reply)
				return
			}
			i++
		}
	})
}
