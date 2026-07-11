package list

import "testing"

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
