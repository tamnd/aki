// Lab: PEL slab layout, one shared record with an owner field vs the dual-PEL
// duplicate (spec 2064/f3 doc 14 section 7.4, M5 lab 03).
//
// The question: doc 14 hangs the pending-entries list off the group header as one
// structure with two indexes over a shared slab. Each pending entry is a 32-byte
// slab record (16B ID, 8B delivery time, 2B delivery count, a 2/4B consumer
// ordinal), and the two indexes point at it by slab ordinal: a Go map hash id ->
// ordinal for the O(1) point ops (XACK, XCLAIM owner lookup) and a counted tree
// keyed (id.ms score, id.seq member) for the O(log p + n) range scans (XPENDING,
// XAUTOCLAIM). The owner lives in the slab, so an ack reads it from the record the
// hash returned and a claim rewrites it in place, never a second id-keyed index.
// Section 7.4 prices this at ~50B per pending: 32B slab, ~8B tree, ~10B hash.
//
// Redis stores a pending entry twice: a streamNACK in the group PEL rax and the
// same NACK pointer in the owning consumer's PEL rax, both keyed by the 16-byte
// ID. That buys range-by-consumer with no ordinal filter, at the cost of a second
// id-keyed ordered structure per pending and a two-tree ack. The memory bar
// PRED-F3-M5-STREAMMEM (and the standing rule that aki holds the same data in less
// RAM than the rivals) turns on this choice, so this lab prices the three layouts
// before slice 6 bakes the shared-slab-with-owner-field form into pel.go.
//
// Three arms, all over the same 32-byte slab so the record cost is held fixed and
// only the index geometry moves:
//
//	A shared slab + counted tree + hash  (the spec's layout): O(1) point, O(log p) range
//	B shared slab + counted tree only    (drop the hash): point ops go through the tree
//	C shared slab + group tree + per-consumer tree (Redis dual): 2x id-keyed nodes
//
// The tree is the real engine counted tree (engine/f3/struct), so the per-pending
// tree and hash cost is measured, not modeled: build N pending, force GC, and read
// the live-heap delta over the slab-only floor. Point-op and range timings run on
// the same structures. Read: resident bytes/pending per layout, XACK ns/op, and
// XPENDING-by-consumer ns/entry. The measured result rejects the spec's hash (arm
// A): it adds ~56 B/pending of pure map overhead and slows XACK ~1.3x for point ops
// the tree already serves, because the tree delete must run regardless, so the PEL
// ships as arm B, tree-only. See README.md for the tables and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	structs "github.com/tamnd/aki/engine/f3/struct"
)

// id is the stream entry ID, the 16 bytes the slab and both index keys carry.
type id struct{ ms, seq uint64 }

// pelEntry is the 32-byte slab record doc 14 section 7.4 fixes: the ID, the idle
// clock, the RETRYCOUNT, and the owning consumer ordinal. Held in a flat arena and
// referenced by ordinal, so an index entry is an ordinal, not a pointer.
type pelEntry struct {
	id            id
	deliveryTime  int64
	deliveryCount uint16
	consumerOrd   uint32
}

// seqKey returns id.seq big-endian in a fresh buffer, the tree member bytes that
// break a same-ms tie in ID order. It matches the real pel.go: the search/insert
// key must not alias the Member scratch, which the tree overwrites mid-descent.
func seqKey(seq uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	return b[:]
}

// --- Arm A: shared slab + counted tree + hash (the spec's layout, rejected) --

type pelA struct {
	slabs []pelEntry
	free  []uint32
	tree  *structs.Tree
	byID  map[id]uint32
	key   [8]byte
}

func newPELA() *pelA {
	return &pelA{tree: structs.NewTree(), byID: make(map[id]uint32)}
}

func (p *pelA) Member(ref uint32) []byte {
	binary.BigEndian.PutUint64(p.key[:], p.slabs[ref].id.seq)
	return p.key[:]
}

func (p *pelA) alloc() uint32 {
	if n := len(p.free); n > 0 {
		ord := p.free[n-1]
		p.free = p.free[:n-1]
		return ord
	}
	p.slabs = append(p.slabs, pelEntry{})
	return uint32(len(p.slabs) - 1)
}

func (p *pelA) insert(e id, now int64, ord uint32) {
	o := p.alloc()
	p.slabs[o] = pelEntry{id: e, deliveryTime: now, deliveryCount: 1, consumerOrd: ord}
	p.tree.Insert(e.ms, seqKey(e.seq), o, p)
	p.byID[e] = o
}

// ack is the O(1) point retire: hash to the ordinal, read the owner off the slab,
// delete from the tree, free the slab. No second id-keyed lookup.
func (p *pelA) ack(e id) (ord uint32, ok bool) {
	o, ok := p.byID[e]
	if !ok {
		return 0, false
	}
	owner := p.slabs[o].consumerOrd
	p.tree.Delete(e.ms, seqKey(e.seq), p)
	delete(p.byID, e)
	p.free = append(p.free, o)
	return owner, true
}

// walkOwner walks the shared tree from start, handing back the pending entries the
// given consumer owns, the XPENDING-by-consumer path: one ordered walk filtered by
// the slab owner field.
func (p *pelA) walkOwner(start id, ord uint32, fn func(*pelEntry)) {
	p.tree.WalkFrom(start.ms, seqKey(start.seq), p, func(_ uint64, ref uint32) bool {
		if p.slabs[ref].consumerOrd == ord {
			fn(&p.slabs[ref])
		}
		return true
	})
}

// --- Arm B: shared slab + counted tree only (the shipped layout) ----------

type pelB struct {
	slabs []pelEntry
	free  []uint32
	tree  *structs.Tree
	key   [8]byte
}

func newPELB() *pelB { return &pelB{tree: structs.NewTree()} }

func (p *pelB) Member(ref uint32) []byte {
	binary.BigEndian.PutUint64(p.key[:], p.slabs[ref].id.seq)
	return p.key[:]
}

func (p *pelB) alloc() uint32 {
	if n := len(p.free); n > 0 {
		ord := p.free[n-1]
		p.free = p.free[:n-1]
		return ord
	}
	p.slabs = append(p.slabs, pelEntry{})
	return uint32(len(p.slabs) - 1)
}

func (p *pelB) insert(e id, now int64, ord uint32) {
	o := p.alloc()
	p.slabs[o] = pelEntry{id: e, deliveryTime: now, deliveryCount: 1, consumerOrd: ord}
	p.tree.Insert(e.ms, seqKey(e.seq), o, p)
}

// ack retires by tree delete alone: the delete locates the record (O(log p)) and
// returns its ordinal, so the owner is still one slab read, but the point op pays a
// tree descent the hash would have saved.
func (p *pelB) ack(e id) (ord uint32, ok bool) {
	o, ok := p.tree.Delete(e.ms, seqKey(e.seq), p)
	if !ok {
		return 0, false
	}
	owner := p.slabs[o].consumerOrd
	p.free = append(p.free, o)
	return owner, true
}

// --- Arm C: shared slab + group tree + per-consumer tree (Redis dual) ------

type pelC struct {
	slabs  []pelEntry
	free   []uint32
	group  *structs.Tree
	byCons map[uint32]*structs.Tree
	key    [8]byte
}

func newPELC() *pelC {
	return &pelC{group: structs.NewTree(), byCons: make(map[uint32]*structs.Tree)}
}

func (p *pelC) Member(ref uint32) []byte {
	binary.BigEndian.PutUint64(p.key[:], p.slabs[ref].id.seq)
	return p.key[:]
}

func (p *pelC) alloc() uint32 {
	if n := len(p.free); n > 0 {
		ord := p.free[n-1]
		p.free = p.free[:n-1]
		return ord
	}
	p.slabs = append(p.slabs, pelEntry{})
	return uint32(len(p.slabs) - 1)
}

func (p *pelC) insert(e id, now int64, ord uint32) {
	o := p.alloc()
	p.slabs[o] = pelEntry{id: e, deliveryTime: now, deliveryCount: 1, consumerOrd: ord}
	p.group.Insert(e.ms, seqKey(e.seq), o, p)
	ct := p.byCons[ord]
	if ct == nil {
		ct = structs.NewTree()
		p.byCons[ord] = ct
	}
	ct.Insert(e.ms, seqKey(e.seq), o, p)
}

// ack retires from both id-keyed trees: the duplicate the dual PEL pays for on
// every acknowledgement.
func (p *pelC) ack(e id, ord uint32) (ok bool) {
	o, ok := p.group.Delete(e.ms, seqKey(e.seq), p)
	if !ok {
		return false
	}
	if ct := p.byCons[p.slabs[o].consumerOrd]; ct != nil {
		ct.Delete(e.ms, seqKey(e.seq), p)
	}
	p.free = append(p.free, o)
	return true
}

// walkOwner walks the consumer's own tree, no owner filter: what the second index
// buys.
func (p *pelC) walkOwner(start id, ord uint32, fn func(*pelEntry)) {
	ct := p.byCons[ord]
	if ct == nil {
		return
	}
	ct.WalkFrom(start.ms, seqKey(start.seq), p, func(_ uint64, ref uint32) bool {
		fn(&p.slabs[ref])
		return true
	})
}

// denseIDs builds n dense auto-IDs, rate per millisecond, the pending set a group
// accumulates under load. consumers spreads ownership round-robin.
func denseIDs(n int, rate uint64) []id {
	ids := make([]id, n)
	var ms, seq uint64
	for i := range ids {
		ids[i] = id{ms: ms, seq: seq}
		seq++
		if seq >= rate {
			seq = 0
			ms++
		}
	}
	return ids
}

// liveHeap returns the live heap in bytes after a GC settle, the figure the
// resident-cost delta reads.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

const consumers = 8

// buildA fills arm A with n pending, ownership round-robin over the consumers, and
// returns it live so the caller can measure and time it.
func buildA(ids []id) *pelA {
	p := newPELA()
	for i, e := range ids {
		p.insert(e, int64(i), uint32(i%consumers))
	}
	return p
}

func buildB(ids []id) *pelB {
	p := newPELB()
	for i, e := range ids {
		p.insert(e, int64(i), uint32(i%consumers))
	}
	return p
}

func buildC(ids []id) *pelC {
	p := newPELC()
	for i, e := range ids {
		p.insert(e, int64(i), uint32(i%consumers))
	}
	return p
}

// memChild builds one arm at one pending count in a clean process and prints the
// live-heap delta over the ids-only floor in bytes per pending. Isolating each
// measurement in its own process keeps the floor clean: in one process the prior
// arm's freed structures leave residue that GC has not returned, which reads a
// later arm below its own slab floor. One arm per process removes that.
func memChild(arm string, n int) {
	ids := denseIDs(n, 1000)
	base := liveHeap()
	var keep any
	switch arm {
	case "slab":
		s := make([]pelEntry, n)
		for i, e := range ids {
			s[i] = pelEntry{id: e, deliveryTime: int64(i), deliveryCount: 1, consumerOrd: uint32(i % consumers)}
		}
		keep = s
	case "A":
		keep = buildA(ids)
	case "B":
		keep = buildB(ids)
	case "C":
		keep = buildC(ids)
	}
	bytes := float64(liveHeap()-base) / float64(n)
	// ids must stay live across the second reading, else it is freed between base
	// and now and the delta reads one id (16B) short per pending.
	runtime.KeepAlive(ids)
	runtime.KeepAlive(keep)
	fmt.Printf("%.1f\n", bytes)
}

// measure re-execs this binary in memChild mode for one arm and count, returning
// the reported bytes per pending.
func measure(self, arm string, n int) float64 {
	out, err := exec.Command(self, "-mem", arm, "-n", strconv.Itoa(n)).Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure %s/%d: %v\n", arm, n, err)
		os.Exit(1)
	}
	v, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return v
}

func main() {
	quick := flag.Bool("quick", false, "smaller pending counts for a fast check")
	mem := flag.String("mem", "", "internal: measure one arm (slab|A|B|C) and exit")
	nChild := flag.Int("n", 0, "internal: pending count for -mem")
	flag.Parse()

	if *mem != "" {
		memChild(*mem, *nChild)
		return
	}

	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("PEL slab-layout sweep, %s\n\n", time.Now().Format("2006-01-02"))

	counts := []int{100_000, 1_000_000}
	if *quick {
		counts = []int{50_000}
	}
	const rate = 1000 // dense auto-IDs, 1000/ms

	// Sweep A: resident bytes per pending, each measured in its own process over the
	// ids-only floor. The slab column is the 32-byte record floor with append slack,
	// the denominator the index columns add to.
	fmt.Println("Sweep A: resident bytes per pending (isolated live-heap delta)")
	fmt.Printf("%-10s %10s %14s %14s %14s\n", "pending", "slab", "A slab+tr+hash", "B slab+tree", "C dual tree")
	for _, n := range counts {
		slab := measure(self, "slab", n)
		aB := measure(self, "A", n)
		bB := measure(self, "B", n)
		cB := measure(self, "C", n)
		fmt.Printf("%-10d %10.1f %14.1f %14.1f %14.1f\n", n, slab, aB, bB, cB)
	}
	fmt.Println()

	// Sweep B: XACK ns/op, the point retire. A hashes, B descends the tree, C
	// deletes from two trees.
	n := counts[0]
	ids := denseIDs(n, rate)
	fmt.Println("Sweep B: XACK ns/op (point retire, whole set acked)")
	{
		a := buildA(ids)
		s := time.Now()
		for _, e := range ids {
			a.ack(e)
		}
		aNs := float64(time.Since(s).Nanoseconds()) / float64(n)

		b := buildB(ids)
		s = time.Now()
		for _, e := range ids {
			b.ack(e)
		}
		bNs := float64(time.Since(s).Nanoseconds()) / float64(n)

		c := buildC(ids)
		s = time.Now()
		for i, e := range ids {
			c.ack(e, uint32(i%consumers))
		}
		cNs := float64(time.Since(s).Nanoseconds()) / float64(n)

		fmt.Printf("%-14s %10.1f\n", "A hash", aNs)
		fmt.Printf("%-14s %10.1f\n", "B tree", bNs)
		fmt.Printf("%-14s %10.1f\n", "C dual tree", cNs)
	}
	fmt.Println()

	// Sweep C: XPENDING-by-consumer ns per pending scanned. A/B walk the shared
	// tree filtering by owner, C walks the consumer's own tree. Report per entry
	// the consumer actually owns, the useful work.
	fmt.Println("Sweep C: XPENDING-by-consumer ns/owned-entry (one consumer's full list)")
	{
		owned := n / consumers
		a := buildA(ids)
		s := time.Now()
		got := 0
		a.walkOwner(id{}, 0, func(*pelEntry) { got++ })
		aNs := float64(time.Since(s).Nanoseconds()) / float64(got)

		c := buildC(ids)
		s = time.Now()
		got = 0
		c.walkOwner(id{}, 0, func(*pelEntry) { got++ })
		cNs := float64(time.Since(s).Nanoseconds()) / float64(got)

		fmt.Printf("owned/consumer %d\n", owned)
		fmt.Printf("%-14s %10.1f  (shared tree, owner-filtered walk)\n", "A/B filter", aNs)
		fmt.Printf("%-14s %10.1f  (per-consumer tree, no filter)\n", "C direct", cNs)
	}
}
