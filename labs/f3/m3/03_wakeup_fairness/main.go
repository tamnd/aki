// Lab: BLPOP/BRPOP waiter-set wakeup fairness policy (spec 2064/f3 doc 13
// section 5.12, M3 list milestone lab 03).
//
// The question: doc 13 parks a blocked list client (BLPOP/BRPOP/BLMOVE/BLMPOP/
// LMPOP) in the owner shard's waiter set for the key. A push checks that set
// inline and serves the longest-waiting client FIFO, building the reply in the
// push's own epoch. The spec's chosen representation is an intrusive doubly
// linked list of per-connection wait nodes hanging off the key's record,
// allocated from the shard arena, zero maps, where FIFO order is just the link
// order so "fairness costs nothing". This lab prices that claim against the
// two plausible alternatives and freezes the wake-cost constant the M3 gate's
// BLPOP latency row is checked against.
//
// Method: in-process, no server, no wire, no engine import. Three waiter-set
// representations, all serving strict FIFO (longest-waiting first):
//
//  1. Intrusive doubly linked list, the spec choice. Nodes come from a
//     preallocated arena (models the shard arena), addressed by uint32 index,
//     no Go pointers into the set and no map. Park appends at the tail, wake
//     dequeues the head, timeout unlinks any node from the middle, all O(1).
//  2. Ring/slice FIFO queue. Park appends to a slice that reallocates and
//     copies as it grows, wake advances a head cursor, timeout finds the
//     victim by scan and removes it by shifting the tail down, which is O(n).
//  3. Map of connection to node, keyed by conn id, FIFO by an auxiliary order
//     slice. This is the allocation and hash cost the spec deliberately
//     avoids: every park hashes and allocates a node, every wake hashes to
//     confirm the head is still live.
//
// The sweep is over waiter count N in {1, 4, 16, 64, 256, 1024}. For each N and
// each representation it reads park ns per waiter, wake ns per waiter under a
// push storm that drains the set oldest first, and timeout-unlink ns per waiter
// when a random middle waiter is removed. Two extra models sit beside the
// sweep: a single push carrying k elements that serves k of N waiters in one
// shot and parks nothing, and a multi-key waiter registered on m keys whose
// first wake unlinks all m siblings so the client is never woken twice, which
// is O(m) not O(N).
//
// Read: the intrusive list wake should be flat in N (head dequeue is O(1)
// regardless of set size) and its timeout unlink flat in N (doubly linked, O(1)
// from the middle), while the ring pays O(n) on the middle removal and the map
// pays a hash and an allocation on every park. See README.md for the tables and
// the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

const nilIdx = ^uint32(0)

// sinkV keeps the compiler from folding away the measured work.
var sinkV uint64

func sink(x uint64) { sinkV += x }

// --- 1. intrusive doubly linked waiter list (the spec choice) ---

// inode is one per-connection wait node. prev and next link the node within its
// key's waiter list; sib forms a circular ring across the keys one client is
// blocked on so a multi-key wake can unlink every sibling; key names the list
// the node lives on so the sibling unlink knows which list to touch.
type inode struct {
	prev, next uint32
	sib        uint32
	key        uint32
	conn       uint64
	live       bool
}

// arena hands out inode slots from a flat slice with a freelist, modelling the
// shard arena the spec allocates wait nodes from. Nodes are addressed by index,
// not by Go pointer, and there is no map.
type arena struct {
	nodes []inode
	free  []uint32
}

func newArena(capacity int) *arena {
	return &arena{nodes: make([]inode, 0, capacity)}
}

func (a *arena) alloc() uint32 {
	if n := len(a.free); n > 0 {
		i := a.free[n-1]
		a.free = a.free[:n-1]
		return i
	}
	i := uint32(len(a.nodes))
	a.nodes = append(a.nodes, inode{})
	return i
}

func (a *arena) release(i uint32) { a.free = append(a.free, i) }

// ilist is the intrusive waiter list for one key: a head and a tail index into
// the arena, plus a live count.
type ilist struct {
	ar   *arena
	head uint32
	tail uint32
	n    int
}

func newIlist(ar *arena) *ilist {
	return &ilist{ar: ar, head: nilIdx, tail: nilIdx}
}

// park appends conn at the tail. O(1): one freelist pop and a few link writes.
func (l *ilist) park(conn uint64) uint32 {
	i := l.ar.alloc()
	nd := &l.ar.nodes[i]
	nd.conn = conn
	nd.live = true
	nd.prev = l.tail
	nd.next = nilIdx
	nd.sib = i
	nd.key = 0
	if l.tail == nilIdx {
		l.head = i
	} else {
		l.ar.nodes[l.tail].next = i
	}
	l.tail = i
	l.n++
	return i
}

// unlinkNode removes node i from wherever it sits in the list. O(1) from the
// middle because the list is doubly linked; this is both the head dequeue and
// the timeout path.
func (l *ilist) unlinkNode(i uint32) {
	nd := &l.ar.nodes[i]
	if !nd.live {
		return
	}
	nd.live = false
	if nd.prev == nilIdx {
		l.head = nd.next
	} else {
		l.ar.nodes[nd.prev].next = nd.next
	}
	if nd.next == nilIdx {
		l.tail = nd.prev
	} else {
		l.ar.nodes[nd.next].prev = nd.prev
	}
	l.n--
	l.ar.release(i)
}

// wake dequeues the head, the longest-waiting client. O(1).
func (l *ilist) wake() (uint64, bool) {
	if l.head == nilIdx {
		return 0, false
	}
	i := l.head
	conn := l.ar.nodes[i].conn
	l.unlinkNode(i)
	return conn, true
}

// serveUpTo hands the k oldest waiters to a push carrying k elements and parks
// nothing. It returns how many were actually served (min of k and the live
// count); the caller pushes any leftover elements as plain list values.
func (l *ilist) serveUpTo(k int, out []uint64) int {
	served := 0
	for served < k {
		c, ok := l.wake()
		if !ok {
			break
		}
		out[served] = c
		served++
	}
	return served
}

// mkset is a small multi-key waiter set built on the same arena, used to model
// and price the multi-key sibling unlink. A client blocked on several keys gets
// one node per key linked into a circular sibling ring; the first wake on any
// key unlinks the whole ring.
type mkset struct {
	ar    *arena
	lists []*ilist
}

func newMkset(ar *arena, keys int) *mkset {
	s := &mkset{ar: ar, lists: make([]*ilist, keys)}
	for i := range s.lists {
		s.lists[i] = newIlist(ar)
	}
	return s
}

// parkMulti registers conn on every key in keys and stitches the nodes into a
// circular sibling ring.
func (s *mkset) parkMulti(conn uint64, keys []int) {
	first := nilIdx
	prev := nilIdx
	for _, k := range keys {
		i := s.lists[k].park(conn)
		s.ar.nodes[i].key = uint32(k)
		if first == nilIdx {
			first = i
		} else {
			s.ar.nodes[prev].sib = i
		}
		prev = i
	}
	if prev != nilIdx {
		s.ar.nodes[prev].sib = first
	}
}

// wakeKey serves the head of key k and unlinks that node and all of its
// siblings across the other keys so the client is never woken twice. It returns
// the served conn and the number of nodes unlinked, which is m (the key count
// the client blocked on), not N.
func (s *mkset) wakeKey(k int) (uint64, int, bool) {
	l := s.lists[k]
	if l.head == nilIdx {
		return 0, 0, false
	}
	i := l.head
	conn := s.ar.nodes[i].conn
	unlinked := 0
	j := i
	for {
		next := s.ar.nodes[j].sib
		key := s.ar.nodes[j].key
		s.lists[key].unlinkNode(j)
		unlinked++
		if next == i {
			break
		}
		j = next
	}
	return conn, unlinked, true
}

// --- 2. ring/slice FIFO waiter queue ---

type rwaiter struct {
	conn uint64
	live bool
}

// ring is a slice-backed FIFO with a head cursor. Park appends (and the slice
// reallocates and copies as it grows), wake advances the head, and timeout
// removal shifts the tail down, which is O(n).
type ring struct {
	buf  []rwaiter
	head int
}

func newRing() *ring { return &ring{} }

func (r *ring) park(conn uint64) {
	r.buf = append(r.buf, rwaiter{conn: conn, live: true})
}

func (r *ring) wake() (uint64, bool) {
	if r.head >= len(r.buf) {
		return 0, false
	}
	w := r.buf[r.head]
	r.head++
	return w.conn, true
}

// unlink removes the waiter with conn by scanning from the head (O(n)) and
// shifting the tail down one (O(n)). This is the cost the spec's doubly linked
// list avoids on the timeout path.
func (r *ring) unlink(conn uint64) bool {
	for i := r.head; i < len(r.buf); i++ {
		if r.buf[i].live && r.buf[i].conn == conn {
			copy(r.buf[i:], r.buf[i+1:])
			r.buf = r.buf[:len(r.buf)-1]
			return true
		}
	}
	return false
}

// --- 3. map of conn to node, FIFO by an auxiliary order slice ---

type mwaiter struct {
	conn uint64
	seq  uint64
}

type mapset struct {
	m     map[uint64]*mwaiter
	order []uint64
	ohead int
	seq   uint64
}

func newMapset() *mapset { return &mapset{m: make(map[uint64]*mwaiter)} }

// park hashes conn into the map and allocates a node, and records the conn in
// the auxiliary order slice that gives FIFO.
func (s *mapset) park(conn uint64) {
	s.seq++
	s.m[conn] = &mwaiter{conn: conn, seq: s.seq}
	s.order = append(s.order, conn)
}

// wake walks the order slice from the front, hashing each conn to skip any that
// already timed out, and serves the first live one.
func (s *mapset) wake() (uint64, bool) {
	for s.ohead < len(s.order) {
		conn := s.order[s.ohead]
		s.ohead++
		if _, ok := s.m[conn]; ok {
			delete(s.m, conn)
			return conn, true
		}
	}
	return 0, false
}

// unlink deletes conn from the map. O(1) on the map, but it leaves a tombstone
// in the order slice that wake pays for later.
func (s *mapset) unlink(conn uint64) bool {
	if _, ok := s.m[conn]; ok {
		delete(s.m, conn)
		return true
	}
	return false
}

// --- measurement ---

func nsPerOp(start time.Time, ops int) float64 {
	return float64(time.Since(start).Nanoseconds()) / float64(ops)
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// park benches. The arena is preallocated for the intrusive list (the shard
// arena is resident), so park there is pointer writes and a freelist pop; the
// ring and map grow from empty on purpose so their reallocation and hashing
// cost shows.

func benchParkIL(n, rounds int) float64 {
	start := time.Now()
	var acc uint64
	for r := 0; r < rounds; r++ {
		ar := newArena(n)
		l := newIlist(ar)
		for i := 0; i < n; i++ {
			l.park(uint64(i) + 1)
		}
		acc += uint64(l.n)
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

func benchParkRing(n, rounds int) float64 {
	start := time.Now()
	var acc uint64
	for r := 0; r < rounds; r++ {
		q := newRing()
		for i := 0; i < n; i++ {
			q.park(uint64(i) + 1)
		}
		acc += uint64(len(q.buf))
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

func benchParkMap(n, rounds int) float64 {
	start := time.Now()
	var acc uint64
	for r := 0; r < rounds; r++ {
		s := newMapset()
		for i := 0; i < n; i++ {
			s.park(uint64(i) + 1)
		}
		acc += uint64(len(s.m))
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

// wake benches. Sets are prebuilt outside the timed section so the timer covers
// only the push storm that drains the set oldest first.

func benchWakeIL(n, rounds int) float64 {
	sets := make([]*ilist, rounds)
	for r := 0; r < rounds; r++ {
		ar := newArena(n)
		l := newIlist(ar)
		for i := 0; i < n; i++ {
			l.park(uint64(i) + 1)
		}
		sets[r] = l
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		l := sets[r]
		for {
			c, ok := l.wake()
			if !ok {
				break
			}
			acc += c
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

func benchWakeRing(n, rounds int) float64 {
	sets := make([]*ring, rounds)
	for r := 0; r < rounds; r++ {
		q := newRing()
		for i := 0; i < n; i++ {
			q.park(uint64(i) + 1)
		}
		sets[r] = q
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		q := sets[r]
		for {
			c, ok := q.wake()
			if !ok {
				break
			}
			acc += c
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

func benchWakeMap(n, rounds int) float64 {
	sets := make([]*mapset, rounds)
	for r := 0; r < rounds; r++ {
		s := newMapset()
		for i := 0; i < n; i++ {
			s.park(uint64(i) + 1)
		}
		sets[r] = s
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		s := sets[r]
		for {
			c, ok := s.wake()
			if !ok {
				break
			}
			acc += c
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

// timeout benches. Each set is drained by unlinking every waiter in a shuffled
// order, so the average unlink lands on a middle position. The intrusive list
// unlinks by node index (O(1)); the ring and map unlink by conn id.

func benchTimeoutIL(n, rounds int, rng *rand.Rand) float64 {
	type built struct {
		l    *ilist
		idxs []uint32
	}
	bs := make([]built, rounds)
	for r := 0; r < rounds; r++ {
		ar := newArena(n)
		l := newIlist(ar)
		idxs := make([]uint32, n)
		for i := 0; i < n; i++ {
			idxs[i] = l.park(uint64(i) + 1)
		}
		rng.Shuffle(n, func(a, b int) { idxs[a], idxs[b] = idxs[b], idxs[a] })
		bs[r] = built{l, idxs}
	}
	start := time.Now()
	for r := 0; r < rounds; r++ {
		l := bs[r].l
		for _, idx := range bs[r].idxs {
			l.unlinkNode(idx)
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(uint64(bs[rounds-1].l.n) + 1)
	return el
}

func benchTimeoutRing(n, rounds int, rng *rand.Rand) float64 {
	type built struct {
		q     *ring
		conns []uint64
	}
	bs := make([]built, rounds)
	for r := 0; r < rounds; r++ {
		q := newRing()
		conns := make([]uint64, n)
		for i := 0; i < n; i++ {
			conns[i] = uint64(i) + 1
			q.park(conns[i])
		}
		rng.Shuffle(n, func(a, b int) { conns[a], conns[b] = conns[b], conns[a] })
		bs[r] = built{q, conns}
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		q := bs[r].q
		for _, c := range bs[r].conns {
			if q.unlink(c) {
				acc++
			}
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

func benchTimeoutMap(n, rounds int, rng *rand.Rand) float64 {
	type built struct {
		s     *mapset
		conns []uint64
	}
	bs := make([]built, rounds)
	for r := 0; r < rounds; r++ {
		s := newMapset()
		conns := make([]uint64, n)
		for i := 0; i < n; i++ {
			conns[i] = uint64(i) + 1
			s.park(conns[i])
		}
		rng.Shuffle(n, func(a, b int) { conns[a], conns[b] = conns[b], conns[a] })
		bs[r] = built{s, conns}
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		s := bs[r].s
		for _, c := range bs[r].conns {
			if s.unlink(c) {
				acc++
			}
		}
	}
	el := nsPerOp(start, n*rounds)
	sink(acc)
	return el
}

// benchMultiKeyWake prices the sibling unlink: a client blocked on m keys, woken
// once, must unlink all m sibling nodes. Background waiters fill each key so the
// lists are not degenerate. Returns ns per wake, which should scale with m.
func benchMultiKeyWake(m, bg, rounds int) float64 {
	bs := make([]*mkset, rounds)
	keys := make([]int, m)
	for i := range keys {
		keys[i] = i
	}
	for r := 0; r < rounds; r++ {
		ar := newArena((bg + 1) * m)
		s := newMkset(ar, m)
		// the multi-key client parks first so it is the head on every key.
		s.parkMulti(1, keys)
		for i := 0; i < bg; i++ {
			for k := 0; k < m; k++ {
				s.lists[k].park(uint64(1000 + i))
			}
		}
		bs[r] = s
	}
	var acc uint64
	start := time.Now()
	for r := 0; r < rounds; r++ {
		_, unl, ok := bs[r].wakeKey(0)
		if ok {
			acc += uint64(unl)
		}
	}
	el := nsPerOp(start, rounds)
	sink(acc)
	return el
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	fmt.Printf("machine: %s/%s, go %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version())

	sizes := []int{1, 4, 16, 64, 256, 1024}
	wakeTarget := 1_000_000
	toTarget := 200_000
	if *quick {
		wakeTarget = 100_000
		toTarget = 40_000
	}
	rng := rand.New(rand.NewSource(0x5eed))

	fmt.Println()
	fmt.Println("waiter-set sweep, ns per waiter (park, wake under push storm, timeout unlink)")
	fmt.Printf("%6s | %8s %8s %8s | %8s %8s %8s | %8s %8s %8s\n",
		"N", "parkIL", "parkRng", "parkMap", "wakeIL", "wakeRng", "wakeMap", "toIL", "toRng", "toMap")
	for _, n := range sizes {
		wr := clamp(wakeTarget/n, 50, 20000)
		tr := clamp(toTarget/n, 20, 5000)

		parkIL := benchParkIL(n, wr)
		parkRng := benchParkRing(n, wr)
		parkMap := benchParkMap(n, wr)

		wakeIL := benchWakeIL(n, wr)
		wakeRng := benchWakeRing(n, wr)
		wakeMap := benchWakeMap(n, wr)

		toIL := benchTimeoutIL(n, tr, rng)
		toRng := benchTimeoutRing(n, tr, rng)
		toMap := benchTimeoutMap(n, tr, rng)

		fmt.Printf("%6d | %8.1f %8.1f %8.1f | %8.1f %8.1f %8.1f | %8.1f %8.1f %8.1f\n",
			n, parkIL, parkRng, parkMap, wakeIL, wakeRng, wakeMap, toIL, toRng, toMap)
		runtime.GC()
	}

	fmt.Println()
	fmt.Println("multi-key sibling unlink, ns per wake as a function of key count m (256 background waiters per key)")
	fmt.Printf("%6s | %10s\n", "m", "wakeNs")
	for _, m := range []int{1, 2, 4, 8, 16} {
		rounds := 20000
		if *quick {
			rounds = 4000
		}
		ns := benchMultiKeyWake(m, 256, rounds)
		fmt.Printf("%6d | %10.1f\n", m, ns)
	}

	if sinkV == 0xffffffffffffffff {
		fmt.Println(sinkV)
	}
}
