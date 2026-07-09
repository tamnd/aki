package setintersect

import (
	"encoding/binary"
	"math/bits"
	"strconv"
	"sync/atomic"
	"testing"
)

// encodeBulk appends one RESP bulk string for m, the model of f1srv's writeBulk:
// a "$<len>\r\n" header (with the strconv the real one runs) plus the payload. A
// SINTER on two 1<<20 half-overlapping sets returns ~1M of these, so at the bench
// scale the reply is a multi-MB serialization, not a rounding error next to the probe.
func encodeBulk(dst, m []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(m)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, m...)
	return append(dst, '\r', '\n')
}

// This lab measures the SINTER inner loop three ways to settle a first-principles
// question: is aki's global-composite-index probe (what f1srv does today) the reason
// SINTER lags, and does a purpose-built compact fingerprint table over the non-driver
// source win enough to matter. None of this imports aki; it models the mechanism so the
// numbers are reproducible and the decision survives.
//
// The fixture is SINTER(A, B) with |A| = |B| = labN and a 50% overlap, the shape the
// real algebra bench (f1srv BenchmarkSInterBig) loads. The driver is A; every strategy
// walks A's members and decides membership in B. Fixtures are built before b.Loop so the
// timed region is only the intersection.

const labN = 1 << 20

// --- shared fixture -------------------------------------------------------------------

func buildMembers(count int) [][]byte {
	ms := make([][]byte, count)
	for i := range ms {
		ms[i] = []byte("member:" + strconv.Itoa(i))
	}
	return ms
}

// intersectFixture returns A and B, each labN members, overlapping in their middle half.
func intersectFixture() (a, b [][]byte) {
	all := buildMembers(labN + labN/2)
	return all[:labN], all[labN/2 : labN/2+labN]
}

// hashBytes is f1raw's index hash (engine/f1raw/f1raw.go hash), copied so the lab probes
// with the same word-at-a-time wyhash the real engine uses.
func hashBytes(b []byte) uint64 {
	const (
		s0 = 0xa0761d6478bd642f
		s1 = 0xe7037ed1a0b428db
		s2 = 0x8ebc6af09c88c6e3
	)
	h := s0 ^ uint64(len(b))
	for len(b) >= 8 {
		h = mulFold(h^binary.LittleEndian.Uint64(b), s1)
		b = b[8:]
	}
	if len(b) > 0 {
		var t uint64
		for i := 0; i < len(b); i++ {
			t |= uint64(b[i]) << (8 * uint(i))
		}
		h = mulFold(h^t, s1)
	}
	return mulFold(h, s2)
}

func mulFold(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func pow2AtLeast(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// --- strategy 1: global composite index (models f1srv today) --------------------------
//
// Every set's members live as records in one shared arena, indexed by one open-addressed
// table keyed on the composite key uvarint(len(skey))|skey|member. A probe rebuilds the
// composite key, hashes the whole thing, scatters into the shared table, and on a tag hit
// follows the slot's arena offset to compare the stored key bytes. Slots are atomic, as
// the real lock-free index requires. dilute inflates the table with extra keys' members so
// the shared index is larger than the two sets under test, the production condition where a
// per-set structure would be far more cache-local than this global one.

type globalIndex struct {
	slots []atomic.Uint64 // tag<<48 | arenaOffset+1 (0 = empty)
	mask  uint64
	arena []byte
}

const gTagShift = 48

func newGlobalIndex(sets [][][]byte, dilute int) *globalIndex {
	total := dilute
	for _, s := range sets {
		total += len(s)
	}
	n := pow2AtLeast(total * 2)
	g := &globalIndex{slots: make([]atomic.Uint64, n), mask: uint64(n - 1)}
	g.arena = make([]byte, 0, total*24)
	// Dilution records under a distinct set name, so the shared table carries other keys'
	// weight the way a real keyspace does.
	if dilute > 0 {
		dil := []byte("dilute")
		for i := range dilute {
			g.insert(dil, []byte("x:"+strconv.Itoa(i)))
		}
	}
	for i, s := range sets {
		skey := []byte("set:" + strconv.Itoa(i))
		for _, m := range s {
			g.insert(skey, m)
		}
	}
	return g
}

// composite writes uvarint(len(skey))|skey|member into dst[:0] and returns it.
func composite(dst, skey, member []byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	dst = append(dst[:0], tmp[:n]...)
	dst = append(dst, skey...)
	dst = append(dst, member...)
	return dst
}

func (g *globalIndex) insert(skey, member []byte) {
	var kb [64]byte
	key := composite(kb[:0], skey, member)
	off := len(g.arena)
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(key)))
	g.arena = append(g.arena, lb[:]...)
	g.arena = append(g.arena, key...)
	h := hashBytes(key)
	tag := (h >> gTagShift) | 1
	w := tag<<gTagShift | uint64(off+1)
	for i := h & g.mask; ; i = (i + 1) & g.mask {
		if g.slots[i].Load() == 0 {
			g.slots[i].Store(w)
			return
		}
	}
}

func (g *globalIndex) exists(skey, member []byte) bool {
	var kb [64]byte
	key := composite(kb[:0], skey, member)
	h := hashBytes(key)
	tag := (h >> gTagShift) | 1
	for i := h & g.mask; ; i = (i + 1) & g.mask {
		w := g.slots[i].Load()
		if w == 0 {
			return false
		}
		if w>>gTagShift != tag {
			continue
		}
		off := int(w&((1<<gTagShift)-1)) - 1
		klen := int(binary.LittleEndian.Uint32(g.arena[off:]))
		start := off + 4
		if string(g.arena[start:start+klen]) == string(key) {
			return true
		}
	}
}

// --- strategy 2: compact fingerprint table over the non-driver source -----------------
//
// Built fresh per operation over B: one open-addressed table of (fingerprint, member)
// sized to |B|, cache-dense, keyed on the hash of the member bytes alone. The driver walk
// hashes each member once, probes the table, and byte-confirms on a fingerprint hit (so a
// 64-bit collision can never corrupt the result the way a fingerprint-only set would).

type fpEntry struct {
	fp     uint64
	member []byte
}

type fpTable struct {
	slots []fpEntry
	mask  uint64
}

func buildFP(members [][]byte) *fpTable {
	n := pow2AtLeast(len(members) * 2)
	t := &fpTable{slots: make([]fpEntry, n), mask: uint64(n - 1)}
	for _, m := range members {
		fp := hashBytes(m)
		if fp == 0 {
			fp = 1
		}
		for i := fp & t.mask; ; i = (i + 1) & t.mask {
			if t.slots[i].fp == 0 {
				t.slots[i] = fpEntry{fp: fp, member: m}
				break
			}
		}
	}
	return t
}

func (t *fpTable) has(member []byte) bool {
	fp := hashBytes(member)
	if fp == 0 {
		fp = 1
	}
	for i := fp & t.mask; ; i = (i + 1) & t.mask {
		e := t.slots[i]
		if e.fp == 0 {
			return false
		}
		if e.fp == fp && string(e.member) == string(member) {
			return true
		}
	}
}

// --- strategy 3: Go map over the non-driver source (models Redis's per-set dict) ------

func buildDict(members [][]byte) map[string]struct{} {
	d := make(map[string]struct{}, len(members))
	for _, m := range members {
		d[string(m)] = struct{}{}
	}
	return d
}

// --- strategy 4: partitioned routing over the shared index (models f1raw doc 19) ------
//
// This is the cost the "40 ns floor" model omits, and the one the real f1srv SINTER pays.
// Doc 19 splits a large set into P partitions, but membership does NOT get a smaller table:
// setMemberExists still probes the ONE shared composite index (f1srv/set_algebra.go:217), it
// just routes first. Per driver member it computes PartitionOf(member, p) = hash(member) & (p-1)
// (a second full member hash, on top of the composite hash the probe already runs), then builds
// a partition-qualified composite uvarint(len(skey))|skey|byte(part)|member (one byte longer than
// the unpartitioned key), then probes the same global table. So the routing tax over the plain
// diluted probe is exactly: one extra member hash and a one-byte-longer composite key. The global
// table stays the same size, so this must be compared against BenchmarkGlobalProbeDiluted (same
// table dilution); the delta is the routing, nothing else.

const labPartitions = 64 // power of two; a 2M set engages many partitions under doc 19

// compositePart writes uvarint(len(skey))|skey|byte(part)|member, f1raw's partMemberKey layout
// (f1srv/set.go partMemberKey), into dst[:0] and returns it.
func compositePart(dst, skey []byte, part int, member []byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	dst = append(dst[:0], tmp[:n]...)
	dst = append(dst, skey...)
	dst = append(dst, byte(part))
	dst = append(dst, member...)
	return dst
}

// insertPart adds member under its routed partition-qualified key, the way a partitioned SADD
// writes it, so a routed probe of the same member resolves the row.
func (g *globalIndex) insertPart(skey, member []byte, p int) {
	part := int(hashBytes(member) & uint64(p-1))
	var kb [64]byte
	key := compositePart(kb[:0], skey, part, member)
	off := len(g.arena)
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(key)))
	g.arena = append(g.arena, lb[:]...)
	g.arena = append(g.arena, key...)
	h := hashBytes(key)
	tag := (h >> gTagShift) | 1
	w := tag<<gTagShift | uint64(off+1)
	for i := h & g.mask; ; i = (i + 1) & g.mask {
		if g.slots[i].Load() == 0 {
			g.slots[i].Store(w)
			return
		}
	}
}

// existsPart reproduces the doc 19 routed probe: hash the member to pick its partition, build the
// partition-qualified composite, then probe the same shared table. The extra member hash and the
// longer key are the whole routing tax this benchmark prices.
func (g *globalIndex) existsPart(skey, member []byte, p int) bool {
	part := int(hashBytes(member) & uint64(p-1))
	var kb [64]byte
	key := compositePart(kb[:0], skey, part, member)
	h := hashBytes(key)
	tag := (h >> gTagShift) | 1
	for i := h & g.mask; ; i = (i + 1) & g.mask {
		w := g.slots[i].Load()
		if w == 0 {
			return false
		}
		if w>>gTagShift != tag {
			continue
		}
		off := int(w&((1<<gTagShift)-1)) - 1
		klen := int(binary.LittleEndian.Uint32(g.arena[off:]))
		start := off + 4
		if string(g.arena[start:start+klen]) == string(key) {
			return true
		}
	}
}

// newPartitionedIndex builds a shared table (diluted like production) holding B's members under
// partition-qualified keys, so it is the exact same global structure as newGlobalIndex(dilute) but
// keyed the way a partitioned set writes. dilute pads it with other keys' members, the condition
// under which the shared table dwarfs cache.
func newPartitionedIndex(members [][]byte, p, dilute int) *globalIndex {
	total := dilute + len(members)
	n := pow2AtLeast(total * 2)
	g := &globalIndex{slots: make([]atomic.Uint64, n), mask: uint64(n - 1)}
	g.arena = make([]byte, 0, total*24)
	if dilute > 0 {
		dil := []byte("dilute")
		for i := range dilute {
			g.insert(dil, []byte("x:"+strconv.Itoa(i)))
		}
	}
	skey := []byte("set:0")
	for _, m := range members {
		g.insertPart(skey, m, p)
	}
	return g
}

// --- benchmarks -----------------------------------------------------------------------

var sink int

// BenchmarkGlobalProbe is the current f1srv shape: probe the shared composite index once
// per driver member. The index holds exactly the two sets (no dilution), the friendliest
// case for the global structure.
func BenchmarkGlobalProbe(b *testing.B) {
	a, bset := intersectFixture()
	g := newGlobalIndex([][][]byte{bset}, 0)
	skeyB := []byte("set:0")
	for b.Loop() {
		n := 0
		for _, m := range a {
			if g.exists(skeyB, m) {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkGlobalProbeDiluted is the production condition: the shared index also carries
// 8x the two sets' worth of other keys' members, so its working set dwarfs L2 and the
// per-probe scatter misses cache the way a real keyspace makes it.
func BenchmarkGlobalProbeDiluted(b *testing.B) {
	a, bset := intersectFixture()
	g := newGlobalIndex([][][]byte{bset}, 8*labN)
	skeyB := []byte("set:0")
	for b.Loop() {
		n := 0
		for _, m := range a {
			if g.exists(skeyB, m) {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkCompactFingerprint is the redesign: build a fresh fingerprint table over B,
// then walk A through it. The build is charged inside the timed loop because a real SINTER
// pays it every call.
func BenchmarkCompactFingerprint(b *testing.B) {
	a, bset := intersectFixture()
	for b.Loop() {
		t := buildFP(bset)
		n := 0
		for _, m := range a {
			if t.has(m) {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkRedisDict models Redis: build a dict over B, probe per driver member. The build
// is charged too, since Redis's set already exists but a fair floor for "member-only probe
// into a per-set structure" includes what it costs to have one.
func BenchmarkRedisDict(b *testing.B) {
	a, bset := intersectFixture()
	for b.Loop() {
		d := buildDict(bset)
		n := 0
		for _, m := range a {
			if _, ok := d[string(m)]; ok {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkCompactFingerprintProbeOnly isolates the probe half of the redesign from its
// build, so the build cost is visible as the gap against BenchmarkCompactFingerprint.
func BenchmarkCompactFingerprintProbeOnly(b *testing.B) {
	a, bset := intersectFixture()
	t := buildFP(bset)
	for b.Loop() {
		n := 0
		for _, m := range a {
			if t.has(m) {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkPartitionedProbe is the real f1srv SINTER inner loop on a partitioned set: for every
// driver member, route through PartitionOf, build the partition-qualified composite, then probe
// the same shared index. Compared against BenchmarkGlobalProbe (unpartitioned, same undiluted
// table) the delta is the routing tax alone: the extra member hash and the one-byte-longer key.
func BenchmarkPartitionedProbe(b *testing.B) {
	a, bset := intersectFixture()
	g := newPartitionedIndex(bset, labPartitions, 0)
	skeyB := []byte("set:0")
	for b.Loop() {
		n := 0
		for _, m := range a {
			if g.existsPart(skeyB, m, labPartitions) {
				n++
			}
		}
		sink = n
	}
}

// BenchmarkPartitionedProbeDiluted is the production condition for the routed probe: the shared
// index also carries 8x the two sets in other keys, so the table dwarfs cache exactly as it does
// for BenchmarkGlobalProbeDiluted. The gap between those two benchmarks is the doc 19 routing tax
// under the real cache pressure, the single cost the "40 ns floor" model left out, and the number
// that says whether unrouting the algebra probe is worth a slice.
func BenchmarkPartitionedProbeDiluted(b *testing.B) {
	a, bset := intersectFixture()
	g := newPartitionedIndex(bset, labPartitions, 8*labN)
	skeyB := []byte("set:0")
	for b.Loop() {
		n := 0
		for _, m := range a {
			if g.existsPart(skeyB, m, labPartitions) {
				n++
			}
		}
		sink = n
	}
}

// sinkReply keeps the encoded reply buffer live so the compiler cannot drop the encode.
var sinkReply int

// BenchmarkFullSInter is the whole command the way f1srv runs it: probe every driver
// member into the shared index, buffer the ~1M hits, then serialize them as a RESP
// array. The gap against BenchmarkGlobalProbe (probe only, same index) is the reply
// encode, and that split is the point of this benchmark. If the encode is the larger
// half, the SINTER 2x lever is the multi-bulk reply path, not the probe structure that
// the rest of this lab already showed is near its memory-bound floor.
func BenchmarkFullSInter(b *testing.B) {
	a, bset := intersectFixture()
	g := newGlobalIndex([][][]byte{bset}, 0)
	skeyB := []byte("set:0")
	buf := make([][]byte, 0, labN)
	out := make([]byte, 0, labN*16)
	for b.Loop() {
		buf = buf[:0]
		for _, m := range a {
			if g.exists(skeyB, m) {
				buf = append(buf, m)
			}
		}
		out = out[:0]
		out = append(out, '*')
		out = strconv.AppendInt(out, int64(len(buf)), 10)
		out = append(out, '\r', '\n')
		for _, m := range buf {
			out = encodeBulk(out, m)
		}
		sink = len(buf)
		sinkReply = len(out)
	}
}

// BenchmarkEncodeOnly isolates just the reply serialization over an already-known
// ~1M-member result, so the encode cost stands alone next to BenchmarkGlobalProbe.
func BenchmarkEncodeOnly(b *testing.B) {
	a, bset := intersectFixture()
	g := newGlobalIndex([][][]byte{bset}, 0)
	skeyB := []byte("set:0")
	result := make([][]byte, 0, labN)
	for _, m := range a {
		if g.exists(skeyB, m) {
			result = append(result, m)
		}
	}
	out := make([]byte, 0, labN*16)
	for b.Loop() {
		out = out[:0]
		out = append(out, '*')
		out = strconv.AppendInt(out, int64(len(result)), 10)
		out = append(out, '\r', '\n')
		for _, m := range result {
			out = encodeBulk(out, m)
		}
		sinkReply = len(out)
	}
}
