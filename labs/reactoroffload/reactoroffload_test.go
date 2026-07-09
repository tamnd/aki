package reactoroffload

import (
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sink keeps the compiler from folding the modeled work away. It is atomic because the offload path's
// pool workers add to it concurrently with the loop goroutine adding its light-op results, the same
// cross-goroutine accumulation the real offload has when a park goroutine runs the heavy compute while
// the reactor loop keeps servicing point ops.
var sink atomic.Uint64

// splitmix64 is a small deterministic PRNG so the fixtures are reproducible without touching the wall
// clock or the global rand source.
type splitmix64 struct{ s uint64 }

func (r *splitmix64) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// memberHash mixes a member's payload into the set-independent uint64 the async folder stores, so the
// same member in two sources yields the same key the two-pointer walk matches.
func memberHash(v uint64) uint64 {
	h := v ^ 0x9e3779b97f4a7c15
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	return h ^ (h >> 33)
}

// heavyOp is one large set-algebra read: two sorted member-hash arrays overlapping in half (so the
// intersection is heavyN/2) plus each member's 64-byte payload, the compute-and-reply the loop runs
// inline today. reply is a reusable buffer the intersect materializes matched members into, standing in
// for the multibulk reply the loop would write.
type heavyOp struct {
	sortedA []uint64
	sortedB []uint64
	members map[uint64][]byte
	reply   [][]byte
}

const heavyN = 256

func buildHeavy(seed uint64) *heavyOp {
	r := splitmix64{s: seed}
	shared := make([]uint64, heavyN/2)
	for i := range shared {
		shared[i] = r.next()
	}
	aVals := append([]uint64(nil), shared...)
	bVals := append([]uint64(nil), shared...)
	for len(aVals) < heavyN {
		aVals = append(aVals, r.next())
	}
	for len(bVals) < heavyN {
		bVals = append(bVals, r.next())
	}
	members := make(map[uint64][]byte, 2*heavyN)
	sortedA := make([]uint64, heavyN)
	sortedB := make([]uint64, heavyN)
	for i, v := range aVals {
		h := memberHash(v)
		sortedA[i] = h
		members[h] = payload(v)
	}
	for i, v := range bVals {
		h := memberHash(v)
		sortedB[i] = h
		members[h] = payload(v)
	}
	slices.Sort(sortedA)
	slices.Sort(sortedB)
	return &heavyOp{sortedA: sortedA, sortedB: sortedB, members: members, reply: make([][]byte, 0, heavyN)}
}

func payload(v uint64) []byte {
	b := make([]byte, 64)
	b[0] = 'm'
	binary.LittleEndian.PutUint64(b[1:9], v)
	return b
}

// run intersects the two sources and materializes the matched members into the reply buffer, the
// compute-plus-reply cost the reactor loop pays inline for one heavy command. It reuses reply so the
// benchmark measures the intersect and pointer append, not buffer allocation.
func (h *heavyOp) run() int {
	h.reply = h.reply[:0]
	i, j := 0, 0
	for i < len(h.sortedA) && j < len(h.sortedB) {
		switch {
		case h.sortedA[i] < h.sortedB[j]:
			i++
		case h.sortedA[i] > h.sortedB[j]:
			j++
		default:
			h.reply = append(h.reply, h.members[h.sortedA[i]])
			i++
			j++
		}
	}
	var acc uint64
	for _, m := range h.reply {
		acc += uint64(m[0])
	}
	return int(acc)
}

// lightWork is one cheap point op: hash an 8-byte key and copy a small value into a reply word, the
// GET-shaped commands that dominate a real batch and whose latency the heavy op inflates when it runs
// inline on the loop.
func lightWork(key uint64) uint64 {
	return memberHash(key) ^ (key << 1)
}

// stream is one batch the loop drains: heavyEvery light ops between each heavy op. Each heavy op gets
// its own fixture so the concurrent pool workers touch distinct arrays, matching independent commands.
type stream struct {
	events  []bool // true = heavy
	heavies []*heavyOp
	keys    []uint64
	nLight  int
	nHeavy  int
}

const (
	streamLen  = 4096
	heavyEvery = 48
)

func buildStream(seed uint64) *stream {
	s := &stream{events: make([]bool, streamLen), keys: make([]uint64, streamLen)}
	r := splitmix64{s: seed}
	hi := 0
	for i := range s.events {
		s.keys[i] = r.next()
		if i%heavyEvery == heavyEvery-1 {
			s.events[i] = true
			s.heavies = append(s.heavies, buildHeavy(seed*1000+uint64(hi)))
			hi++
			s.nHeavy++
		} else {
			s.nLight++
		}
	}
	return s
}

// pool is the worker set the offload loop hands heavy ops to. submit never blocks the loop for long: the
// channel is buffered past the heavy count in one stream, so the loop drops the op and moves to the next
// light op, exactly the responsiveness the offload buys.
type pool struct {
	ch   chan *heavyOp
	wg   sync.WaitGroup
	done atomic.Int64
}

func newPool(workers, cap int) *pool {
	p := &pool{ch: make(chan *heavyOp, cap)}
	for range workers {
		p.wg.Go(func() {
			for h := range p.ch {
				sink.Add(uint64(h.run()))
				p.done.Add(1)
			}
		})
	}
	return p
}

func (p *pool) submit(h *heavyOp) { p.ch <- h }
func (p *pool) stop()             { close(p.ch); p.wg.Wait() }

// runInline drains the stream the way the reactor does today: light ops and heavy ops both run on the
// loop goroutine in arrival order. It returns the light-makespan, the elapsed time to the loop finishing
// the last light op, which carries every heavy op's compute the loop ran before it.
func runInline(s *stream) time.Duration {
	start := time.Now()
	var lastLight time.Duration
	hi := 0
	for i, heavy := range s.events {
		if heavy {
			sink.Add(uint64(s.heavies[hi].run()))
			hi++
			continue
		}
		sink.Add(lightWork(s.keys[i]))
		lastLight = time.Since(start)
	}
	return lastLight
}

// runOffload drains the stream with the heavy ops handed to the pool: the loop runs each light op and
// submits each heavy op without waiting, so the last light op is serviced without queuing behind any
// heavy compute. It returns the same light-makespan for comparison against runInline.
func runOffload(s *stream, p *pool) time.Duration {
	start := time.Now()
	var lastLight time.Duration
	hi := 0
	for i, heavy := range s.events {
		if heavy {
			p.submit(s.heavies[hi])
			hi++
			continue
		}
		sink.Add(lightWork(s.keys[i]))
		lastLight = time.Since(start)
	}
	return lastLight
}

// BenchmarkReactorInline measures the loop's occupancy draining a batch with heavy set-algebra ops run
// inline: ns/op is the whole stream's loop time (lights + heavies) and light-makespan-ns is how long the
// last light op waited, which includes every heavy op's compute the loop ran first.
func BenchmarkReactorInline(b *testing.B) {
	s := buildStream(0x5eed)
	var makespan time.Duration
	var iters int64
	b.ReportAllocs()
	for b.Loop() {
		makespan += runInline(s)
		iters++
	}
	b.ReportMetric(float64(makespan.Nanoseconds())/float64(iters), "light-makespan-ns")
}

// BenchmarkReactorOffload measures the loop's occupancy draining the same batch with the heavy ops handed
// to a worker pool: ns/op drops to lights + the handoff and light-makespan-ns collapses to roughly the
// light work alone, the loop responsiveness the offload restores. The pool runs GOMAXPROCS-1 workers, the
// spare cores the loop is not on, and its channel is sized past one stream's heavy count so submit does
// not stall the loop.
func BenchmarkReactorOffload(b *testing.B) {
	s := buildStream(0x5eed)
	p := newPool(4, len(s.heavies)*8+8)
	defer p.stop()
	var makespan time.Duration
	var iters int64
	b.ReportAllocs()
	for b.Loop() {
		makespan += runOffload(s, p)
		iters++
	}
	b.ReportMetric(float64(makespan.Nanoseconds())/float64(iters), "light-makespan-ns")
}

// TestHeavyIntersectExact pins that the modeled heavy op returns the heavyN/2 intersection, so a
// benchmark that looks fast because the intersect dropped members is caught.
func TestHeavyIntersectExact(t *testing.T) {
	h := buildHeavy(1)
	h.run()
	if got := len(h.reply); got != heavyN/2 {
		t.Fatalf("intersect = %d members, want %d", got, heavyN/2)
	}
}

// TestOffloadRunsAllHeavy pins that offloading drops no heavy work: after the loop submits every heavy op
// and the pool drains, the pool has run exactly the stream's heavy count, so the benchmark's freed-loop
// figure is honest and not the result of skipped compute.
func TestOffloadRunsAllHeavy(t *testing.T) {
	s := buildStream(0x5eed)
	p := newPool(4, len(s.heavies)*2+2)
	runOffload(s, p)
	p.stop()
	if got := int(p.done.Load()); got != s.nHeavy {
		t.Fatalf("pool ran %d heavy ops, want %d", got, s.nHeavy)
	}
}
