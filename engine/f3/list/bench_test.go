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
