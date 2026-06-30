package command

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// The spilled-collection benchmarks are the internal, CI-runnable proxy for the
// larger-than-memory regime (spec 2064/ltm/07 section 4). The external aki-bench
// scenario proves the LTM win under a real cgroup cap; these prove the same point
// read stays bounded and keeps faulting cheaply when the buffer pool is a sliver of
// the sub-tree, with no cgroup and no separate process. A regression that turns a
// descent into an O(n) scan, or starts allocating per element, shows up here as a
// throughput collapse and a jump in ReportAllocs long before the external run.
//
// The shape that makes the build cheap and the probe honest: members carry an
// ascending %08d prefix, so SADD/HSET/ZADD insert in key order and the build
// touches only the rightmost leaf and its parents (a handful of pages that fit even
// the tiny pool). The probe then reads a uniformly random member, so each call is a
// fresh root-to-leaf descent that has to fault interior and leaf pages back through
// the capped pool. Build stays resident-friendly; reads genuinely spill.
const (
	spilledN    = 200000 // members in the one probed collection (~50MB raw at 248B each)
	spilledPad  = 240    // padding per member, past the listpack threshold into coll form
	spilledPool = 64     // buffer-pool frames; 64 * 4KB = 256KB resident over ~50MB
)

// newSpilledDispatcher builds a dispatcher over a disk-backed keyspace whose buffer
// pool holds only cachePages frames. Unlike newFuzzDispatcher (mem VFS, default
// pool) this opens a real file so the spill is genuine: pages evicted from the pool
// are read back from disk, not from a resident mem buffer.
func newSpilledDispatcher(tb testing.TB, cachePages int) *Dispatcher {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "data.aki")
	p, err := pager.Create(vfs.NewOS(), path, pager.Options{PageSize: 4096, DBCount: 16, CachePages: cachePages})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		tb.Fatalf("open keyspace: %v", err)
	}
	return New(Config{Engine: NewEngine(ks)})
}

// spilledMembers builds n padded members with an ascending prefix. The slice is
// held resident by the test (like BenchmarkGetSpilled's preloaded key set); the
// spill is in the collection's pages, not the probe key list.
func spilledMembers(n int) [][]byte {
	pad := make([]byte, spilledPad)
	for i := range pad {
		pad[i] = 'x'
	}
	ms := make([][]byte, n)
	for i := range ms {
		ms[i] = []byte(fmt.Sprintf("%08d", i) + string(pad))
	}
	return ms
}

// probeBench runs a uniformly random point read over the prebuilt collection. The
// xorshift index keeps the access deterministic and allocation-free so ReportAllocs
// reflects only the command path, not the key selection.
func probeBench(b *testing.B, d *Dispatcher, cmd string, key []byte, ms [][]byte) {
	conn := networking.NewOfflineConn()
	c := []byte(cmd)
	n := uint32(len(ms))
	b.ReportAllocs()
	b.ResetTimer()
	var x uint32 = 2463534242
	for i := 0; i < b.N; i++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		conn.ResetOut()
		d.Handle(conn, [][]byte{c, key, ms[x%n]})
	}
}

// assertEncoding builds confidence the collection is in coll form, not a listpack,
// so the benchmark measures the sub-tree descent and not an in-place small-encoding
// scan that would never spill.
func assertEncoding(b *testing.B, d *Dispatcher, key, want string) {
	conn := networking.NewOfflineConn()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte(key)})
	got := string(conn.OutBytes())
	if got != want {
		b.Fatalf("%s not in coll form: OBJECT ENCODING = %q, want %q", key, got, want)
	}
}

func BenchmarkCollSISMEMBERSpilled(b *testing.B) {
	d := newSpilledDispatcher(b, spilledPool)
	ms := spilledMembers(spilledN)
	conn := networking.NewOfflineConn()
	key := []byte("s")
	for _, m := range ms {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SADD"), key, m})
	}
	assertEncoding(b, d, "s", "$9\r\nhashtable\r\n")
	probeBench(b, d, "SISMEMBER", key, ms)
}

func BenchmarkCollHGetSpilled(b *testing.B) {
	d := newSpilledDispatcher(b, spilledPool)
	ms := spilledMembers(spilledN)
	conn := networking.NewOfflineConn()
	key := []byte("h")
	val := []byte("v")
	for _, m := range ms {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HSET"), key, m, val})
	}
	assertEncoding(b, d, "h", "$9\r\nhashtable\r\n")
	probeBench(b, d, "HGET", key, ms)
}

func BenchmarkCollZScoreSpilled(b *testing.B) {
	d := newSpilledDispatcher(b, spilledPool)
	ms := spilledMembers(spilledN)
	conn := networking.NewOfflineConn()
	key := []byte("z")
	for i, m := range ms {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), key, []byte(strconv.Itoa(i)), m})
	}
	assertEncoding(b, d, "z", "$8\r\nskiplist\r\n")
	probeBench(b, d, "ZSCORE", key, ms)
}

func BenchmarkCollZRankSpilled(b *testing.B) {
	d := newSpilledDispatcher(b, spilledPool)
	ms := spilledMembers(spilledN)
	conn := networking.NewOfflineConn()
	key := []byte("z")
	for i, m := range ms {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("ZADD"), key, []byte(strconv.Itoa(i)), m})
	}
	assertEncoding(b, d, "z", "$8\r\nskiplist\r\n")
	probeBench(b, d, "ZRANK", key, ms)
}
