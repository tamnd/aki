package set

import (
	"encoding/binary"
	"testing"
)

// The STORE microbenchmarks (doc 11 section 7, PRED-F3-M1-STORE). Each builds two
// sources at half overlap and times the destination build alone (the source build
// is off the clock), reporting ns per result member: the bulk-build cost the
// setstorebuild fix is meant to make linear, against f1's 0.30x-0.55x. These are
// darwin microbenchmarks and directional only; the gate rows key on the GamingPC
// server numbers where the merge lever and the memory bar are read.

// members16Range returns n distinct 16-byte members starting at index lo, so two
// ranges can be built to overlap by construction. The members are binary, not
// integer text, so a large result lands the native hashtable band, the STORE
// destination shape the 1M rows exercise.
func members16Range(lo, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b, uint64(lo+i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(b[8:], uint64(lo+i))
		out[i] = b
	}
	return out
}

func setFromBytes(ms [][]byte) *set {
	s := newSet(ms[0])
	for _, m := range ms {
		s.add(m)
	}
	return s
}

// benchStore builds two n-member sources overlapping by half and times one STORE
// build of the given form, reporting ns per result member.
func benchStore(b *testing.B, n int, op string) {
	sa := setFromBytes(members16Range(0, n))
	sb := setFromBytes(members16Range(n/2, n))
	sets := []*set{sa, sb}
	// Result sizes: intersection n/2, union 1.5n, diff n/2.
	var resultN int
	switch op {
	case "inter", "diff":
		resultN = n / 2
	case "union":
		resultN = n + n/2
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s *set
		switch op {
		case "inter":
			s = storeResult(minCard(sets), func(e func([]byte)) { sinter(sets, e) })
		case "union":
			s = storeResult(totalCard(sets), func(e func([]byte)) { unionInto(sets, e) })
		case "diff":
			s = storeResult(firstCard(sets), func(e func([]byte)) { sdiff(sets, e) })
		}
		sink = s != nil
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*resultN), "ns/member")
}

func BenchmarkSInterStore10k(b *testing.B) { benchStore(b, 10_000, "inter") }
func BenchmarkSInterStore1M(b *testing.B)  { benchStore(b, 1_000_000, "inter") }
func BenchmarkSUnionStore10k(b *testing.B) { benchStore(b, 10_000, "union") }
func BenchmarkSUnionStore1M(b *testing.B)  { benchStore(b, 1_000_000, "union") }
func BenchmarkSDiffStore10k(b *testing.B)  { benchStore(b, 10_000, "diff") }
func BenchmarkSDiffStore1M(b *testing.B)   { benchStore(b, 1_000_000, "diff") }
