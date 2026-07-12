package list

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The waiter set's hot ops are park and the O(1) teardown a serve or a timeout
// runs, the frozen lab 03 metric reproduced in-tree. An anchor waiter keeps the
// list resident so the measured loop times the steady state a busy blocked key
// holds: a node off the recycle stack, the FIFO link, and the sibling-ring
// unlink, with no per-waiter allocation.

func BenchmarkWaiterParkUnlink(b *testing.B) {
	g := &reg{m: make(map[string]*list), waiters: make(map[string]*waitList)}
	c := &shard.Conn{}
	keys := [][]byte{[]byte("k")}
	spec := waitSpec{kind: kindPop, front: true}
	_ = parkWaiter(g, keys, spec, c, 0) // anchor
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.unlinkAll(nil, parkWaiter(g, keys, spec, c, 1))
	}
}

func BenchmarkWaiterParkUnlinkMultiKey(b *testing.B) {
	g := &reg{m: make(map[string]*list), waiters: make(map[string]*waitList)}
	c := &shard.Conn{}
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	spec := waitSpec{kind: kindPop, front: true}
	for _, k := range keys { // anchor every list so none is created in the loop
		_ = parkWaiter(g, [][]byte{k}, spec, c, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.unlinkAll(nil, parkWaiter(g, keys, spec, c, 1))
	}
}
