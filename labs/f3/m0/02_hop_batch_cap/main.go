// Lab: hop batch cap (spec 2064/f3/03 section 3.2, M0 lab 2).
//
// The question: how many commands should one hop batch node carry? Doc 03
// starts at batchCap = 32 and pre-registers a {16, 32, 64} sweep (PRED-X8);
// this lab widens the sweep to {1, 4, 8, 16, 32, 64, 128} and measures both
// sides of the trade on a simulated cross-shard hop load: throughput (bigger
// batches amortize the producer's tail-swap atomic and the consumer's pop
// over more commands) against in-queue latency (a command parked in a
// half-built batch, and head-of-line behind its batchmates, waits longer as
// the cap grows).
//
// Method: one consumer worker owns an engine/f3/store and drains a Vyukov
// intrusive MPSC queue of batch nodes, executing each command as a GET, in
// two regimes: a cache-warm 64k keyspace where the GET is cheap and queue
// costs show, and a memory-bound 1M keyspace where the GET dominates. Four
// producers (the simulated net cores) fill batch
// nodes of cap B with random keys and publish each node with one atomic
// swap, exactly the hop transport's publish discipline. Outstanding work is
// bounded per producer in commands, not nodes, so queue depth is the same at
// every B and the latency column isolates the batching effect. Every 64th
// command carries an enqueue timestamp taken when it is written into the
// node; the consumer records completion time. Total command count is fixed
// per configuration.
//
// See README.md for the numbers and the verdict.
package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	valBytes       = 64
	nProducers     = 4
	cmdsPerProd    = 1 << 21 // 2M commands per producer, 8M total
	outstandingCmd = 1024    // per-producer in-flight command bound
	sampleEvery    = 64
)

type hopCmd struct {
	key   uint32
	stamp int64 // ns enqueue timestamp, 0 when unsampled
}

// hopBatch is the queue node: intrusive MPSC link, a done flag the producer
// polls to recycle the node, and up to cap inline commands.
type hopBatch struct {
	next atomic.Pointer[hopBatch]
	done atomic.Bool
	n    int
	cmds []hopCmd
}

// mpsc is the Vyukov intrusive MPSC queue: producers swap the tail, the sole
// consumer walks next pointers with plain loads.
type mpsc struct {
	head *hopBatch
	tail atomic.Pointer[hopBatch]
}

func newMPSC(bcap int) *mpsc {
	stub := &hopBatch{cmds: make([]hopCmd, bcap)}
	q := &mpsc{head: stub}
	q.tail.Store(stub)
	return q
}

func (q *mpsc) push(b *hopBatch) {
	b.next.Store(nil)
	prev := q.tail.Swap(b)
	prev.next.Store(b)
}

// pop returns the next batch's payload or nil. The head node in a Vyukov
// queue doubles as the stub, so the node that just arrived cannot leave the
// structure yet; instead its payload is swapped into the vacated old head,
// which is fully unlinked, and that node is returned. A node is therefore
// handed back to its producer one pop late, and a node resting as the empty
// queue's head/tail is never handed back at all, which is exactly the
// lifetime the done flag needs. The nil-next window (a producer between its
// two stores) reads as empty and the caller retries.
func (q *mpsc) pop() *hopBatch {
	h := q.head
	next := h.next.Load()
	if next == nil {
		return nil
	}
	q.head = next
	h.cmds, next.cmds = next.cmds, h.cmds
	h.n = next.n
	return h
}

func keyOf(i uint32) []byte {
	var b [16]byte
	x := uint64(i) + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	for j := 0; j < 16; j++ {
		b[j] = "0123456789abcdef"[(x>>(j*4))&15]
	}
	return b[:]
}

type result struct {
	mops float64
	p50  time.Duration
	p99  time.Duration
}

func runCap(s *store.Store, nKeys, bcap int) result {
	q := newMPSC(bcap)
	total := nProducers * cmdsPerProd
	var lat []int64
	done := make(chan struct{})

	// Consumer: the owner worker. Drains batches, executes GETs, marks nodes
	// done for recycling.
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		dst := make([]byte, 0, valBytes)
		seen := 0
		for seen < total {
			b := q.pop()
			if b == nil {
				continue
			}
			for i := 0; i < b.n; i++ {
				c := &b.cmds[i]
				var ok bool
				dst, ok = s.Get(keyOf(c.key), dst)
				if !ok {
					panic("miss")
				}
				if c.stamp != 0 {
					lat = append(lat, time.Now().UnixNano()-c.stamp)
				}
			}
			seen += b.n
			b.done.Store(true)
		}
		close(done)
	}()

	var wg sync.WaitGroup
	t0 := time.Now()
	for p := 0; p < nProducers; p++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			rng := rand.New(rand.NewSource(seed))
			// Node ring sized so outstanding commands, not nodes, are
			// constant across caps.
			nNodes := outstandingCmd / bcap
			if nNodes < 2 {
				nNodes = 2
			}
			ring := make([]*hopBatch, nNodes)
			for i := range ring {
				ring[i] = &hopBatch{cmds: make([]hopCmd, bcap)}
				ring[i].done.Store(true)
			}
			sent, ri := 0, 0
			for sent < cmdsPerProd {
				// Scan for any done node rather than blocking on one: the
				// node resting as the empty queue's head is handed back only
				// after a later push, so waiting on it specifically could
				// stall; with two or more nodes some other one is done.
				for !ring[ri].done.Load() {
					ri = (ri + 1) % nNodes
				}
				b := ring[ri]
				ri = (ri + 1) % nNodes
				b.done.Store(false)
				n := bcap
				if left := cmdsPerProd - sent; left < n {
					n = left
				}
				for i := 0; i < n; i++ {
					c := &b.cmds[i]
					c.key = uint32(rng.Intn(nKeys))
					c.stamp = 0
					if (sent+i)%sampleEvery == 0 {
						c.stamp = time.Now().UnixNano()
					}
				}
				b.n = n
				q.push(b)
				sent += n
			}
		}(int64(bcap)*131 + int64(p))
	}
	wg.Wait()
	<-done
	el := time.Since(t0)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return result{
		mops: float64(total) / el.Seconds() / 1e6,
		p50:  time.Duration(lat[len(lat)/2]),
		p99:  time.Duration(lat[len(lat)*99/100]),
	}
}

func main() {
	fmt.Printf("cores=%d producers=%d val=%dB cmds=%d outstanding=%d cmds/producer\n",
		runtime.NumCPU(), nProducers, valBytes, nProducers*cmdsPerProd, outstandingCmd)
	val := make([]byte, valBytes)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	// Two consumer regimes: a cache-warm 64k keyspace where the GET is tens
	// of nanoseconds and the queue's per-batch costs are visible, and a 1M
	// keyspace where the GET misses DRAM and dominates everything.
	for _, nKeys := range []int{1 << 16, 1 << 20} {
		s := store.New(256<<20, 0)
		for i := uint32(0); i < uint32(nKeys); i++ {
			if err := s.Set(keyOf(i), val); err != nil {
				panic(err)
			}
		}
		fmt.Printf("\nkeys=%d\n\n", nKeys)
		fmt.Println("| cap | Mcmds/s | p50 in-queue | p99 in-queue |")
		fmt.Println("|---|---|---|---|")
		for _, c := range []int{1, 4, 8, 16, 32, 64, 128} {
			r := runCap(s, nKeys, c)
			fmt.Printf("| %d | %.1f | %v | %v |\n", c, r.mops, r.p50, r.p99)
		}
	}
}
