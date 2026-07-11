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
