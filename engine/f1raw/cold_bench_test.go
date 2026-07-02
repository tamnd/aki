package f1raw

import (
	"path/filepath"
	"testing"
)

// Collection kind bytes, copied from the f1srv package so the engine benches probe the
// exact record kinds the wire path stores under (f1srv/hash.go, set.go, zset.go). The
// engine is kind-agnostic, so any distinct byte works, but matching the server's bytes
// keeps these benches honest about the layout HGET/SISMEMBER/ZSCORE/ZRANK really touch.
const (
	benchKindHashField  byte = 0x01
	benchKindSetMember  byte = 0x02
	benchKindZsetMember byte = 0x03
)

// The larger-than-memory read path has no wire and no cgroup here: these benches and
// the alloc gate measure the engine-side cold read in isolation, the internal proxy
// for the LTM regime the harness spec (2064/ltm/07 section 4) calls for. A cold read
// is one pread of an immutable separated value plus a DONTNEED of exactly the range
// read, with no seqlock and no lock, so its cost must be a bounded constant per read
// regardless of how many keys are on disk, and it must not allocate once the caller's
// destination buffer is warm. A regression that reintroduces an allocation or turns
// the point read into a scan shows up here before the external cgroup run catches it.

// filledColdStore builds a store whose values all live on the cold log: every value is
// valLen bytes and the separation threshold is 1, so nothing stays inline and every
// Get resolves through the cold pread path. The arena only has to hold the index,
// record headers, keys, and 12-byte cold pointers, never the values, which is the
// whole point of key-value separation, so it is sized for the pointer records alone.
func filledColdStore(tb testing.TB, keys, valLen int) *Store {
	tb.Helper()
	buckets := keys / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(16, ptrSize))
	arena := keys*rec + keys*rec/4 + 1<<20
	path := filepath.Join(tb.TempDir(), "cold-bench.vlog")
	s, err := NewWithCold(buckets, arena, path, 1)
	if err != nil {
		tb.Fatalf("NewWithCold: %v", err)
	}
	val := make([]byte, valLen)
	var kb [16]byte
	for i := 0; i < keys; i++ {
		k := makeKey(kb[:], uint64(i))
		if err := s.Set(k, val); err != nil {
			tb.Fatalf("Set: %v", err)
		}
	}
	return s
}

// BenchmarkGetColdSeparated is the LTM string read floor: a uniform-random point read
// of a value that lives on disk, dst reused across iterations so the steady state is
// one pread with no Go allocation. b.SetBytes reports the value bandwidth the pread
// path sustains. Compare against BenchmarkGet (the inline in-memory read) to read the
// separation tax directly.
func BenchmarkGetColdSeparated(b *testing.B) {
	const keys = 1 << 16 // 65,536 values on the cold log
	const valLen = 4096  // a 4 KiB value, a realistic LTM payload well over the cutoff
	s := filledColdStore(b, keys, valLen)
	defer s.Close()
	var kb [16]byte
	var dst []byte
	b.SetBytes(valLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], uint64(i)&(keys-1))
		var ok bool
		dst, ok = s.Get(k, dst)
		if !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetColdSeparatedParallel scales the cold read across GOMAXPROCS. The pread
// path carries its own file offset (pread, not a shared cursor) and a separated record
// is immutable, so distinct-key cold reads share no mutable state and the curve should
// stay flat per core rather than flatten under a lock. Each goroutine keeps its own dst
// so the steady state is allocation-free.
//
//	go test -bench GetColdSeparatedParallel -cpu 1,2,4,8 ./engine/f1raw/
func BenchmarkGetColdSeparatedParallel(b *testing.B) {
	const keys = 1 << 16
	const valLen = 4096
	s := filledColdStore(b, keys, valLen)
	defer s.Close()
	b.SetBytes(valLen)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var kb [16]byte
		var dst []byte
		var i uint64
		for pb.Next() {
			k := makeKey(kb[:], i&(keys-1))
			var ok bool
			dst, ok = s.Get(k, dst)
			if !ok {
				b.Fatal("miss")
			}
			i++
		}
	})
}

// TestColdReadIsBoundedAlloc is the standing alloc gate on the cold read path: with a
// warm destination buffer a separated read must allocate a small constant, not scale
// with anything, so the read stays a bounded pread and cannot silently regress into a
// path that copies or grows per call. readInto reuses dst when its capacity fits, so
// the steady-state cold read allocates nothing on the Go heap; the gate allows a small
// slack for the runtime rather than asserting an exact zero.
func TestColdReadIsBoundedAlloc(t *testing.T) {
	const keys = 1 << 12
	const valLen = 4096
	s := filledColdStore(t, keys, valLen)
	defer s.Close()
	var kb [16]byte
	dst := make([]byte, valLen) // warm the buffer so the pread reuses it
	var i uint64
	avg := testing.AllocsPerRun(2000, func() {
		k := makeKey(kb[:], i&(keys-1))
		var ok bool
		dst, ok = s.Get(k, dst)
		if !ok {
			t.Fatal("miss")
		}
		i++
	})
	if avg > 1 {
		t.Fatalf("cold read allocates %.2f/op, want a bounded constant <= 1 (warm dst should be reused)", avg)
	}
}

// collElemKey writes a composite element key (collection name then a fixed-width member
// index) into dst[:0] and returns it, the shape f1srv builds for a hash field, set member,
// or zset member row. Fixed-width members keep the probe cost constant across iterations.
func collElemKey(dst []byte, coll string, member uint64) []byte {
	dst = append(dst[:0], coll...)
	var mb [16]byte
	return append(dst, makeKey(mb[:], member)...)
}

// The next four benches are the collection point reads under the larger-than-memory
// regime, the spilled-collection proxies the harness spec (2064/ltm/07 section 4) names:
// BenchmarkCollHGetSpilled, BenchmarkCollSISMEMBERSpilled, BenchmarkCollZScoreSpilled,
// BenchmarkCollZRankSpilled. They exist to answer one question the element-per-row model
// makes sharp: when a collection's values are large enough to spill to the cold log, which
// of its point reads pay the pread tax and which stay resident? The answer is structural,
// and only one of the four pays it.
//
// A hash field row (kindHashField) carries the field's value, so HGET reads it and a large
// value spills, so HGET is the one collection point read that pays the cold tax. A set
// member row (kindSetMember) has an empty value: the member is the key tail, so SISMEMBER
// is a pure index probe (ExistsKind) that never reads a value and so never spills. A zset
// member row (kindZsetMember) carries an 8-byte score, far under any realistic separation
// threshold, so ZSCORE stays inline no matter how large the dataset. ZRANK is an
// order-statistic descent over the ordered index (CollRankOf), reading no value at all.
// So three of the four collection point reads are resident by construction and only HGET
// descends to disk, which is the property that keeps membership, rank, and score reads
// cheap in the LTM regime a value-in-line design would make pay for the spill.

// BenchmarkCollHGetSpilled is the one collection point read that pays the cold tax: a hash
// field whose value is large enough to live on the cold log, so HGET (GetKind) resolves
// through one pread. Compare against BenchmarkGetColdSeparated (the string cold read) to
// confirm a hash field pays the same separation tax and no more, and against
// BenchmarkCollSISMEMBERSpilled to see the resident-versus-cold split directly.
func BenchmarkCollHGetSpilled(b *testing.B) {
	const fields = 1 << 16
	const valLen = 4096 // over any realistic threshold, so every field value spills
	const coll = "h:big"
	buckets := fields / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(len(coll)+16, ptrSize))
	arena := fields*rec + fields*rec/4 + 1<<20
	path := filepath.Join(b.TempDir(), "coll-hget.vlog")
	s, err := NewWithCold(buckets, arena, path, 1) // threshold 1: every field value separates
	if err != nil {
		b.Fatalf("NewWithCold: %v", err)
	}
	defer s.Close()
	val := make([]byte, valLen)
	var kb [64]byte
	for i := 0; i < fields; i++ {
		if _, err := s.PutKind(collElemKey(kb[:], coll, uint64(i)), val, benchKindHashField); err != nil {
			b.Fatalf("PutKind: %v", err)
		}
	}
	var dst []byte
	b.SetBytes(valLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := collElemKey(kb[:], coll, uint64(i)&(fields-1))
		var ok bool
		dst, ok = s.GetKind(k, dst, benchKindHashField)
		if !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkCollSISMEMBERSpilled proves set membership pays no cold tax even when the store
// runs the LTM tier. Set member rows carry an empty value, so SISMEMBER (ExistsKind) is a
// pure index probe: nothing to separate, nothing to pread. The store opens a cold log with
// threshold 1 to put it in the exact LTM configuration BenchmarkCollHGetSpilled uses; the
// difference in the two numbers is the whole point, membership stays index-resident.
func BenchmarkCollSISMEMBERSpilled(b *testing.B) {
	const members = 1 << 16
	const coll = "s:big"
	buckets := members / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(len(coll)+16, ptrSize))
	arena := members*rec + members*rec/4 + 1<<20
	path := filepath.Join(b.TempDir(), "coll-sismember.vlog")
	s, err := NewWithCold(buckets, arena, path, 1)
	if err != nil {
		b.Fatalf("NewWithCold: %v", err)
	}
	defer s.Close()
	var kb [64]byte
	for i := 0; i < members; i++ {
		if _, err := s.PutKind(collElemKey(kb[:], coll, uint64(i)), nil, benchKindSetMember); err != nil {
			b.Fatalf("PutKind: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := collElemKey(kb[:], coll, uint64(i)&(members-1))
		if !s.ExistsKind(k, benchKindSetMember) {
			b.Fatal("miss")
		}
	}
}

// BenchmarkCollZScoreSpilled shows a zset score stays inline in the LTM tier: a score is
// 8 bytes, far under the default separation threshold, so ZSCORE (GetKind of the member
// row's score value) never spills to the cold log no matter how large the dataset grows.
// The store opens with the engine default threshold (0 selects it), the realistic LTM
// configuration, and the read resolves straight from the arena.
func BenchmarkCollZScoreSpilled(b *testing.B) {
	const members = 1 << 16
	const coll = "z:big"
	buckets := members / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(len(coll)+16, 8))
	arena := members*rec + members*rec/4 + 1<<20
	path := filepath.Join(b.TempDir(), "coll-zscore.vlog")
	s, err := NewWithCold(buckets, arena, path, 0) // default threshold: an 8-byte score stays inline
	if err != nil {
		b.Fatalf("NewWithCold: %v", err)
	}
	defer s.Close()
	var score [8]byte
	var kb [64]byte
	for i := 0; i < members; i++ {
		if _, err := s.PutKind(collElemKey(kb[:], coll, uint64(i)), score[:], benchKindZsetMember); err != nil {
			b.Fatalf("PutKind: %v", err)
		}
	}
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := collElemKey(kb[:], coll, uint64(i)&(members-1))
		var ok bool
		dst, ok = s.GetKind(k, dst, benchKindZsetMember)
		if !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkCollZRankSpilled shows ZRANK stays resident in the LTM tier: a rank is an
// order-statistic descent over the ordered element index (CollRankOf), which reads no
// value at all, so it cannot spill and its cost is O(log n) in the cardinality regardless
// of value size. The members are inserted into the ordered index (CollInsert), the same
// step the wire path runs after each member write, so the rank descent has real spans.
func BenchmarkCollZRankSpilled(b *testing.B) {
	const members = 1 << 16
	const coll = "z:rank"
	buckets := members / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(len(coll)+16, 8))
	arena := members*rec + members*rec/4 + 1<<20
	path := filepath.Join(b.TempDir(), "coll-zrank.vlog")
	s, err := NewWithCold(buckets, arena, path, 0)
	if err != nil {
		b.Fatalf("NewWithCold: %v", err)
	}
	defer s.Close()
	var score [8]byte
	var kb [64]byte
	for i := 0; i < members; i++ {
		k := collElemKey(kb[:], coll, uint64(i))
		if _, err := s.PutKind(k, score[:], benchKindZsetMember); err != nil {
			b.Fatalf("PutKind: %v", err)
		}
		s.CollInsert(k, benchKindZsetMember)
	}
	prefix := []byte(coll)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := collElemKey(kb[:], coll, uint64(i)&(members-1))
		if r := s.CollRankOf(prefix, k); r < 0 {
			b.Fatal("missing rank")
		}
	}
}

// TestSpilledMembershipStaysResident locks the architectural invariant the four benches
// above measure: in the LTM tier (a cold log with threshold 1, so any non-empty value
// would spill), a set membership probe reads no value and so allocates nothing and cannot
// touch the cold log. If a future change made SISMEMBER materialize a value or take a copy
// path, this gate trips before the external cgroup run would catch the regression.
func TestSpilledMembershipStaysResident(t *testing.T) {
	const members = 1 << 12
	const coll = "s:res"
	buckets := members / 4
	rec := int(recSize(len(coll)+16, ptrSize))
	arena := members*rec + members*rec/4 + 1<<20
	path := filepath.Join(t.TempDir(), "coll-resident.vlog")
	s, err := NewWithCold(buckets, arena, path, 1)
	if err != nil {
		t.Fatalf("NewWithCold: %v", err)
	}
	defer s.Close()
	var kb [64]byte
	for i := 0; i < members; i++ {
		if _, err := s.PutKind(collElemKey(kb[:], coll, uint64(i)), nil, benchKindSetMember); err != nil {
			t.Fatalf("PutKind: %v", err)
		}
	}
	var i uint64
	avg := testing.AllocsPerRun(2000, func() {
		k := collElemKey(kb[:], coll, i&(members-1))
		if !s.ExistsKind(k, benchKindSetMember) {
			t.Fatal("miss")
		}
		i++
	})
	if avg > 0 {
		t.Fatalf("spilled SISMEMBER allocates %.2f/op, want 0 (membership is index-only, never reads a value)", avg)
	}
}
