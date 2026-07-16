package list

import (
	"strconv"
	"testing"
)

// Steady-state ns for the inline hot point ops, measured over the packed blob
// the way the zero-alloc proofs exercise it: a warmed list whose capacity has
// settled, so the numbers are the in-place op cost, not arena growth.

func BenchmarkLLen(b *testing.B) {
	l := warmList(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkInt = l.length()
	}
}

func BenchmarkLIndex(b *testing.B) {
	l := warmList(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkBytes = l.inlineAt(32)
	}
}

func BenchmarkRPushRPop(b *testing.B) {
	l := warmList(64)
	v := []byte("elem")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.inlinePushBack(v)
		sinkBytes = l.inlinePopBack()
	}
}

func BenchmarkLPushLPop(b *testing.B) {
	l := warmList(64)
	l.inlinePopFront() // open a dead prefix
	v := []byte("elem")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.inlinePushFront(v)
		sinkBytes = l.inlinePopFront()
	}
}

// The native deque edge ops, measured at growing element counts (hence chunk
// counts): the ns must stay flat as the ring deepens, since an established end
// op never touches the directory or leaves the end chunk (spec 2064/f3/13
// section 2.5). 100000 elements at 4-byte values is ~780 chunks, well past the
// flatMax crossover, so the flat number here confirms the ends do not pay the
// directory depth.

func buildNative(n int) *native {
	nt := &native{}
	for i := 0; i < n; i++ {
		nt.pushBack([]byte("elem"))
	}
	return nt
}

func benchNativeSizes(b *testing.B, pair func(nt *native, v []byte)) {
	v := []byte("elem")
	for _, n := range []int{128, 4096, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			nt := buildNative(n)
			// Settle the end chunks so the first measured op is steady state.
			for i := 0; i < 8; i++ {
				pair(nt, v)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pair(nt, v)
			}
		})
	}
}

func BenchmarkNativeRPushRPop(b *testing.B) {
	benchNativeSizes(b, func(nt *native, v []byte) {
		nt.pushBack(v)
		sinkBytes = nt.popBack()
	})
}

func BenchmarkNativeLPushLPop(b *testing.B) {
	benchNativeSizes(b, func(nt *native, v []byte) {
		nt.pushFront(v)
		sinkBytes = nt.popFront()
	})
}

// The positional seek path across the flat-versus-Fenwick crossover (spec
// 2064/f3/13 section 2.4, applying lab 02's frozen FLAT_MAX=128). LINDEX, LSET,
// and the LRANGE start all ride one locate: a flat linear scan at or below 128
// chunks, a Fenwick rank descent above it. The benchmarks size the ring at 32
// chunks (below the crossover, flat), 512 and 4096 chunks (well above, Fenwick),
// and seek a pseudo-random index, so the ns/op traces the crossover shape.
//
// Measured on an Apple M4 (darwin/arm64, go1.26.5), one run, ns/op:
//
//	                              32ch     512ch    4096ch
//	BenchmarkSeekDirect/flat      21.6     176.4    1959.0    linear in chunks
//	BenchmarkSeekDirect/fenwick   18.2      30.8      40.6    ~flat, log in chunks
//	BenchmarkLINDEX               24.7      34.7      46.0    wired path, Fenwick >128
//	BenchmarkLSET                 26.6      36.9      48.8    select + in-place write
//	BenchmarkLRANGEStart          21.9      31.3      42.6    start-index select only
//
// Verdict: above the crossover the Fenwick descent is at least as fast as flat
// and pulls away as the ring grows (512 chunks: 30.8 vs 176.4 ns; 4096 chunks:
// 40.6 vs 1959.0, about 48x), which is lab 02's shape. One honest note against the
// lab: the real flat scan walks chunk structs through a ring modulo and a pointer
// deref per step, heavier than lab 02's idealized scan over a contiguous []uint64,
// so here the two paths are already even at 32 chunks (flat 21.6 vs Fenwick 18.2)
// and the true crossover sits a little below 128. Applying the frozen 128 is still
// safe: at and below it the flat scan is short, and above it Fenwick wins by a
// widening margin. Every wired seek stays well under half the old dense point read
// (~100ns) at all three sizes, so the doc 13 line 444 reopener does not trip.

// benchIdx is a deterministic xorshift so a seek benchmark walks a spread of
// indices without a divide-heavy counter and without allocating.
type benchIdx uint64

func (x *benchIdx) at(mod int) int {
	v := uint64(*x)
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = benchIdx(v)
	return int(v % uint64(mod))
}

// benchSeekSizes runs fn at ring sizes straddling the crossover: 32 chunks is
// below flatMax (flat), 512 and 4096 are well above (Fenwick).
func benchSeekSizes(b *testing.B, fn func(b *testing.B, nt *native)) {
	for _, chunks := range []int{32, 512, 4096} {
		nt := buildNative(chunks * chunkElemCap)
		b.Run(strconv.Itoa(chunks), func(b *testing.B) {
			fn(b, nt)
		})
	}
}

// BenchmarkSeekDirect times the two directory paths head to head at each ring
// size, forcing the flat scan even above the crossover so the crossover shape is
// visible in one package: flat climbs linearly, Fenwick stays near flat.
func BenchmarkSeekDirect(b *testing.B) {
	b.Run("flat", func(b *testing.B) {
		benchSeekSizes(b, func(b *testing.B, nt *native) {
			x := benchIdx(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ci, _ := forceFlatLocate(nt, x.at(nt.count))
				sinkInt = ci
			}
		})
	})
	b.Run("fenwick", func(b *testing.B) {
		benchSeekSizes(b, func(b *testing.B, nt *native) {
			nt.dir.sync(&nt.ring)
			x := benchIdx(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ci, _ := nt.dir.rank(x.at(nt.count))
				sinkInt = ci
			}
		})
	})
}

// BenchmarkLINDEX times the wired positional read: locate picks flat at 32 chunks
// and the Fenwick descent at 512 and 4096.
func BenchmarkLINDEX(b *testing.B) {
	benchSeekSizes(b, func(b *testing.B, nt *native) {
		x := benchIdx(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkBytes = nt.at(x.at(nt.count))
		}
	})
}

// BenchmarkLSET times a same-length overwrite: one locate then an in-place frame
// write, so it measures the seek plus the cheap write, no rebuild.
func BenchmarkLSET(b *testing.B) {
	v := []byte("ELEM") // same length as buildNative's "elem", so setAt writes in place
	benchSeekSizes(b, func(b *testing.B, nt *native) {
		x := benchIdx(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nt.setAt(x.at(nt.count), v)
		}
	})
}

// BenchmarkLRANGEStart times the start-index resolution an LRANGE pays before it
// streams: one locate, the seek term the doc prices separately from the scan.
func BenchmarkLRANGEStart(b *testing.B) {
	benchSeekSizes(b, func(b *testing.B, nt *native) {
		x := benchIdx(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ci, _ := nt.locate(x.at(nt.count))
			sinkInt = ci
		}
	})
}

// BenchmarkPromoteInlineToNative prices the one-way inline-to-deque promotion at
// the budget edge: adopting a full listpack blob into chunk 0 and building the
// directory in one pass (section 4.3).
func BenchmarkPromoteInlineToNative(b *testing.B) {
	val := make([]byte, 80)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		l := newList()
		for j := 0; j < 98; j++ { // a listpack right under the 8 KiB budget
			l.inlinePushBack(val)
		}
		b.StartTimer()
		l.toNative()
		sinkInt = l.length()
	}
}

// The interior-edit surgery against the whole-list rebuild it replaces (spec
// 2064/f3/13 sections 5.6 to 5.8). Each benchmark builds a fresh 50000-element
// deque under the stopped timer, then times one edit: the "surgery" arm runs the
// bounded in-chunk repack or chunk-range delete, the "rebuild" arm runs the old
// toSlice plus rebuild placeholder on the same input. The build is under the
// stopped timer, so the reported allocs/op is the edit's own cost: the surgery
// touches a handful of small header slices, the rebuild clones every element
// through toSlice (about 50000 allocs, one per element).
//
// Measured on an Apple M4 (darwin/arm64, go1.26.5), one run:
//
//	                             ns/op    allocs/op
//	BenchmarkLINSERT/surgery     67516            8    pivot scan + in-chunk repack/split
//	BenchmarkLINSERT/rebuild   1090749        51156    toSlice + splice + full rebuild
//	BenchmarkLREM/surgery       140521            6    count-signed scan + per-hit repack
//	BenchmarkLREM/rebuild      1615242        51180    toSlice + removeMatches + rebuild
//	BenchmarkLTRIM/surgery      101314            5    chunk-range delete, keep a window
//	BenchmarkLTRIM/rebuild      459185        50005    toSlice + rebuild of the window
//
// Verdict: the surgery is bounded by CAP and the match or dropped count, not by
// the list length, so its allocs/op stays a single-digit constant while the
// rebuild allocs one clone per element (about 6000x to 8500x fewer allocations).
// On ns the surgery is 16x faster for LINSERT, 11x for LREM, and 4.5x for LTRIM.
// LINSERT and LREM still pay the irreducible value scan the doc prices at parity
// with Redis (the mid-list pivot walk for LINSERT, the full-list victim walk for
// remove-all LREM), so their remaining ns is that scan plus the O(CAP) surgery,
// the deleted term being the O(n) rebuild and per-element renumber. LTRIM's
// surgery ns is dominated by the O(dropped) byte-accounting walk over the chunks
// it unlinks, still well under the rebuild that clones all 50000 through toSlice.

// benchInteriorN is the element count the surgery-versus-rebuild benchmarks build,
// large enough that the O(n) rebuild is visibly expensive against the bounded edit.
const benchInteriorN = 50000

// benchInterior builds a fresh deque from vals under the stopped timer and times
// op, so the reported ns/op is the edit cost and the build is not double counted.
func benchInterior(b *testing.B, vals [][]byte, op func(nt *native)) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nt := buildNativeVals(vals)
		b.StartTimer()
		op(nt)
	}
}

// insertVals is the 50000-element input with distinct 4-byte values and a known
// pivot near the middle.
func insertVals() ([][]byte, []byte) {
	vals := make([][]byte, benchInteriorN)
	for i := range vals {
		vals[i] = sized(8, i)
	}
	pivot := append([]byte(nil), vals[benchInteriorN/2]...)
	return vals, pivot
}

func BenchmarkLINSERT(b *testing.B) {
	vals, pivot := insertVals()
	nv := []byte("NEWV")
	b.Run("surgery", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { nt.insert(true, pivot, nv) })
	})
	b.Run("rebuild", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { insertViaRebuild(nt, pivot, nv) })
	})
}

// insertViaRebuild is the placeholder LINSERT the surgery replaces: it
// materializes the whole list, splices, and repacks from empty.
func insertViaRebuild(nt *native, pivot, v []byte) {
	all := nt.toSlice()
	idx := -1
	for i, e := range all {
		if bytesEqual(e, pivot) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	all = append(all, nil)
	copy(all[idx+1:], all[idx:])
	all[idx] = cloneBytes(v)
	nt.rebuild(all)
}

// remVals sprinkles a repeated victim token through the 50000-element list so a
// removal finds a handful of matches spread across chunks.
func remVals() [][]byte {
	vals := make([][]byte, benchInteriorN)
	for i := range vals {
		if i%10000 == 5000 {
			vals[i] = []byte("RM")
			continue
		}
		vals[i] = sized(8, i)
	}
	return vals
}

func BenchmarkLREM(b *testing.B) {
	vals := remVals()
	victim := []byte("RM")
	b.Run("surgery", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { nt.remove(0, victim) })
	})
	b.Run("rebuild", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { removeViaRebuild(nt, 0, victim) })
	})
}

// removeViaRebuild is the placeholder LREM the surgery replaces.
func removeViaRebuild(nt *native, count int, v []byte) {
	all := nt.toSlice()
	kept, removed := removeMatches(all, count, v)
	if removed > 0 {
		nt.rebuild(kept)
	}
}

func BenchmarkLTRIM(b *testing.B) {
	vals := make([][]byte, benchInteriorN)
	for i := range vals {
		vals[i] = sized(8, i)
	}
	lo, hi := benchInteriorN/2-50, benchInteriorN/2+50 // a ~100-element middle window
	b.Run("surgery", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { nt.trim(lo, hi) })
	})
	b.Run("rebuild", func(b *testing.B) {
		benchInterior(b, vals, func(nt *native) { trimViaRebuild(nt, lo, hi) })
	})
}

// trimViaRebuild is the placeholder LTRIM the surgery replaces.
func trimViaRebuild(nt *native, start, stop int) {
	all := nt.toSlice()
	nt.rebuild(all[start : stop+1])
}

// The LPOS scan, native band, against the per-index walk it replaces (spec
// 2064/f3/13 section 5.9). native.lpos walks the chunk frames contiguously and
// carries the absolute position as the running element count, so it pays no
// per-element directory seek; the old shape resolved every index through
// locate, which above the flat/Fenwick crossover is an O(log chunks) rank
// descent per element. Each arm builds a 100000-element deque (~782 chunks, well
// past flatMax) and times a full scan: forward_tail seeks a token at the very
// tail (the worst case for a head-to-tail walk, it compares every element),
// backward_head seeks a token at the head with RANK -1 (the worst case for a
// tail-to-head walk), and count_all collects every match of a token sprinkled
// every 1000 positions with COUNT 0. The "contig" arm is the shipped native.lpos;
// the "byindex" arm is the old per-element locate walk, kept here for the A/B.
//
// Measured on an Apple M4 (darwin/arm64, go1.26.5), one run, 100000 elements:
//
//	                              contig ns/op   byindex ns/op   contig ns/elem
//	BenchmarkLPOS/forward_tail       301245        1326127          3.01
//	BenchmarkLPOS/backward_head      295623        1328524          2.96
//	BenchmarkLPOS/count_all          310923        1311041          3.11
//
// Verdict: the contiguous walk is ~3.0 ns per element compared, the low
// single-digit ns the note 29 scan-cost lab predicts (~2.9ns/elem), while the
// per-index walk pays ~13.2 ns per element, about 4.4x more, because it resolves
// every position through locate on top of the same compare. The win is the
// deleted per-element directory seek, not a cheaper compare: both arms run the
// same length-gated bytesEqual, and both allocate only the small result slice
// (1 alloc for a single-hit reply, 8 for the count-all growth). One honest note:
// the byindex arm here is the deque's own per-element locate (a resident Fenwick
// descent, ~13 ns), not f1's ~70 ns dense-model hash probe, so this A/B prices
// the seek the native scan deletes rather than restaging the f1 24x gap; the
// gate row measures aki against Redis's quicklist, which pays the same contiguous
// listpack walk this arm now matches.

const benchLposN = 100000

// buildNativeLpos builds a benchLposN-scale deque of 4-byte fillers with the
// 3-byte target token placed where want reports true, matching the differential's
// element shape.
func buildNativeLpos(want func(i int) bool, target, filler []byte) *native {
	nt := &native{}
	for i := 0; i < benchLposN; i++ {
		if want(i) {
			nt.pushBack(target)
		} else {
			nt.pushBack(filler)
		}
	}
	return nt
}

func BenchmarkLPOS(b *testing.B) {
	target := []byte("TGT")
	filler := []byte("elem")
	forwardTail := buildNativeLpos(func(i int) bool { return i == benchLposN-1 }, target, filler)
	backwardHead := buildNativeLpos(func(i int) bool { return i == 0 }, target, filler)
	countAll := buildNativeLpos(func(i int) bool { return i%1000 == 0 }, target, filler)

	b.Run("forward_tail", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = forwardTail.lpos(target, 1, 1, 0)
		}
	})
	b.Run("backward_head", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = backwardHead.lpos(target, -1, 1, 0)
		}
	})
	b.Run("count_all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = countAll.lpos(target, 1, 0, 0)
		}
	})
}

// BenchmarkLPOSByIndex is the A/B baseline: the same three scans over the same
// geometries, but resolving every position through locate the way the old
// get(i)-per-element lposScan did. It exists only to price the seek the shipped
// scan deletes.
// The same-shard move (spec 2064/f3/13 M3 slice 6), priced on the native band so
// the pop and the push each ride the deque's O(1) end path. BenchmarkLMOVE times
// a cross-key RIGHT LEFT move and then moves the element straight back, keeping
// both lists native and steady, so the reported ns/op is one round trip of two
// moves. BenchmarkRPOPLPUSH times a same-key RIGHT LEFT rotation, the tail-to-head
// move that never changes the list length. This slice prices no new constant, so
// it is a package benchmark and not a labs/f3 microbenchmark (the slice-1
// precedent: only a slice that freezes a constant earns a lab).

func BenchmarkLMOVE(b *testing.B) {
	g, cx := newMoveReg()
	g.m["a"] = seedList(bigVals("a", 300)...)
	g.m["b"] = seedList(bigVals("b", 300)...)
	ka, kb := []byte("a"), []byte("b")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lmove(g, cx, ka, kb, false, true)
		lmove(g, cx, kb, ka, false, true)
	}
}

func BenchmarkRPOPLPUSH(b *testing.B) {
	g, cx := newMoveReg()
	g.m["k"] = seedList(bigVals("k", 300)...)
	kk := []byte("k")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lmove(g, cx, kk, kk, false, true)
	}
}

// BenchmarkLMPOP prices the non-blocking multi-key pop on the native band (spec
// 2064/f3/13 M3 slice 8). It pops one element off the head of a many-chunk list
// each iteration, reusing the reply buffer so the steady path allocates nothing,
// and reseeds under a stopped timer when the list drains, so the reported ns/op
// is the key-selection plus O(1) front pop plus reply build, not the reseed. Like
// the move benchmarks this slice prices no new constant, so it is a package
// benchmark and not a labs/f3 microbenchmark (the slice-1 precedent: only a slice
// that freezes a constant earns a lab).
func BenchmarkLMPOP(b *testing.B) {
	g, cx := newMoveReg()
	keys := bb("a", "b")
	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if g.m["a"] == nil {
			b.StopTimer()
			g.m["a"] = seedList(bigVals("a", 4096)...) // native band, ~ a few hundred chunks
			b.StartTimer()
		}
		out, _, _ := lmpop(g, cx, buf[:0], keys, true, 1)
		buf = out
	}
}

func BenchmarkLPOSByIndex(b *testing.B) {
	target := []byte("TGT")
	filler := []byte("elem")
	forwardTail := buildNativeLpos(func(i int) bool { return i == benchLposN-1 }, target, filler)
	backwardHead := buildNativeLpos(func(i int) bool { return i == 0 }, target, filler)
	countAll := buildNativeLpos(func(i int) bool { return i%1000 == 0 }, target, filler)

	b.Run("forward_tail", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = lposByIndex(forwardTail, target, 1, 1, 0)
		}
	})
	b.Run("backward_head", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = lposByIndex(backwardHead, target, -1, 1, 0)
		}
	})
	b.Run("count_all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sinkHits = lposByIndex(countAll, target, 1, 0, 0)
		}
	})
}

// lposByIndex is the old per-index LPOS walk the contiguous scan replaces: it
// resolves each position through nt.at, which routes through locate, so a long
// ring pays a directory seek per element. Kept in the benchmark only, for the
// A/B against native.lpos.
func lposByIndex(nt *native, target []byte, rank, limit, maxlen int) []int {
	forward := rank > 0
	skip := rank
	if skip < 0 {
		skip = -skip
	}
	skip--
	n := nt.count
	var out []int
	compared := 0
	visit := func(i int) bool {
		if maxlen > 0 && compared >= maxlen {
			return false
		}
		compared++
		if !bytesEqual(nt.at(i), target) {
			return true
		}
		if skip > 0 {
			skip--
			return true
		}
		out = append(out, i)
		return limit <= 0 || len(out) < limit
	}
	if forward {
		for i := 0; i < n; i++ {
			if !visit(i) {
				break
			}
		}
	} else {
		for i := n - 1; i >= 0; i-- {
			if !visit(i) {
				break
			}
		}
	}
	return out
}
