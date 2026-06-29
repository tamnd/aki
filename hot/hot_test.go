package hot

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func newStore(t *testing.T, shards int) *Store {
	t.Helper()
	s, err := New(Tunables{Shards: shards})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func get(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, ok, err := s.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	return string(v), ok
}

// TestNewRejectsBadShards checks the only error New returns.
func TestNewRejectsBadShards(t *testing.T) {
	for _, n := range []int{0, -1, 3, 100} {
		if _, err := New(Tunables{Shards: n}); err == nil {
			t.Fatalf("New(Shards=%d) want error", n)
		}
	}
	if _, err := New(Tunables{Shards: 1}); err != nil {
		t.Fatalf("New(Shards=1): %v", err)
	}
}

// TestSetGetDelete covers the point-path basics across enough shards that keys
// land on different ones: insert, read back, overwrite, miss, and delete.
func TestSetGetDelete(t *testing.T) {
	s := newStore(t, 16)

	if _, ok := get(t, s, "absent"); ok {
		t.Fatal("absent key reported present")
	}

	prev, err := s.SetWithPrev([]byte("k"), []byte("hello"))
	if err != nil || prev != -1 {
		t.Fatalf("SetWithPrev new = (%d,%v) want (-1,nil)", prev, err)
	}
	if v, ok := get(t, s, "k"); !ok || v != "hello" {
		t.Fatalf("Get k = (%q,%v) want (hello,true)", v, ok)
	}

	prev, _ = s.SetWithPrev([]byte("k"), []byte("worldwide"))
	if prev != len("hello") {
		t.Fatalf("overwrite prev = %d want %d", prev, len("hello"))
	}
	if v, ok := get(t, s, "k"); !ok || v != "worldwide" {
		t.Fatalf("Get k after overwrite = (%q,%v)", v, ok)
	}

	n, ok, err := s.DeleteWithPrev([]byte("k"))
	if err != nil || !ok || n != len("worldwide") {
		t.Fatalf("DeleteWithPrev = (%d,%v,%v)", n, ok, err)
	}
	if _, ok := get(t, s, "k"); ok {
		t.Fatal("key present after delete")
	}
	if del, _ := s.Delete([]byte("k")); del {
		t.Fatal("second delete reported present")
	}
}

// TestManyKeysGrow inserts well past the initial table capacity to exercise grow
// and confirms every key reads back, then deletes half and confirms the split.
func TestManyKeysGrow(t *testing.T) {
	s := newStore(t, 8)
	const n = 50000

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key:%d", i)
		if err := s.Set([]byte(k), []byte(fmt.Sprintf("val:%d", i))); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	if s.Len() != n {
		t.Fatalf("Len = %d want %d", s.Len(), n)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("val:%d", i)
		if v, ok := get(t, s, fmt.Sprintf("key:%d", i)); !ok || v != want {
			t.Fatalf("Get key:%d = (%q,%v) want %q", i, v, ok, want)
		}
	}

	for i := 0; i < n; i += 2 {
		if ok, _ := s.Delete([]byte(fmt.Sprintf("key:%d", i))); !ok {
			t.Fatalf("Delete key:%d missing", i)
		}
	}
	if s.Len() != n/2 {
		t.Fatalf("Len after deletes = %d want %d", s.Len(), n/2)
	}
	for i := 0; i < n; i++ {
		_, ok := get(t, s, fmt.Sprintf("key:%d", i))
		if wantPresent := i%2 == 1; ok != wantPresent {
			t.Fatalf("key:%d present=%v want %v", i, ok, wantPresent)
		}
	}
}

// TestNoLeakOnOverwrite is the regression test for the store/ bug. Overwriting one
// key a million times must hold one entry's worth of memory, not a million, and
// the index table must not have grown, because each write drops the old entry.
func TestNoLeakOnOverwrite(t *testing.T) {
	s := newStore(t, 1)
	const rounds = 1_000_000

	val := make([]byte, 64)
	for i := 0; i < rounds; i++ {
		val[0] = byte(i)
		if err := s.Set([]byte("hot"), val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	if s.Len() != 1 {
		t.Fatalf("Len = %d want 1", s.Len())
	}
	// One key of 3 + 64 bytes plus a fixed overhead. Anything near rounds*record
	// would mean the append-only leak. Give generous slack but far below a leak.
	if mb := s.MemBytes(); mb > 4*1024 {
		t.Fatalf("MemBytes = %d after %d overwrites, want bounded (<4KiB)", mb, rounds)
	}
	if ib := s.IndexBytes(); ib > 1024 {
		t.Fatalf("IndexBytes = %d, table grew under overwrite-only load", ib)
	}
	if v, ok := get(t, s, "hot"); !ok || len(v) != 64 {
		t.Fatalf("Get hot = (len %d,%v)", len(v), ok)
	}
}

// TestNoLeakOnDeleteChurn inserts and deletes the same keys many times and
// confirms tombstone-clearing rehash keeps the table from growing without bound.
func TestNoLeakOnDeleteChurn(t *testing.T) {
	s := newStore(t, 1)
	for round := 0; round < 2000; round++ {
		for i := 0; i < 64; i++ {
			k := []byte(fmt.Sprintf("k:%d", i))
			_ = s.Set(k, []byte("v"))
		}
		for i := 0; i < 64; i++ {
			k := []byte(fmt.Sprintf("k:%d", i))
			s.Delete(k)
		}
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d want 0", s.Len())
	}
	// 64 live keys at peak; the table should be small, not sized to the ~128k
	// total inserts churned through it.
	if ib := s.IndexBytes(); ib > 8*1024 {
		t.Fatalf("IndexBytes = %d, churn grew the table", ib)
	}
}

// TestEachAndClear checks enumeration sees every live key and Clear empties.
func TestEachAndClear(t *testing.T) {
	s := newStore(t, 4)
	want := map[string]string{}
	for i := 0; i < 1000; i++ {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		s.Set([]byte(k), []byte(v))
		want[k] = v
	}
	s.Delete([]byte("k500"))
	delete(want, "k500")

	seen := map[string]string{}
	s.Each(func(k, v []byte) bool {
		seen[string(k)] = string(v)
		return true
	})
	if len(seen) != len(want) {
		t.Fatalf("Each saw %d keys want %d", len(seen), len(want))
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("Each[%s] = %q want %q", k, seen[k], v)
		}
	}

	// Early stop.
	count := 0
	s.Each(func(k, v []byte) bool { count++; return count < 10 })
	if count != 10 {
		t.Fatalf("Each early-stop visited %d want 10", count)
	}

	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Len() != 0 || s.MemBytes() != 0 {
		t.Fatalf("after Clear Len=%d MemBytes=%d want 0,0", s.Len(), s.MemBytes())
	}
	if _, ok := get(t, s, "k1"); ok {
		t.Fatal("key present after Clear")
	}
}

// TestValueSliceStableAcrossOverwrite confirms the read contract: a value slice
// returned by Get stays valid and unchanged even after the key is overwritten,
// because a write publishes a new entry rather than editing the old one.
func TestValueSliceStableAcrossOverwrite(t *testing.T) {
	s := newStore(t, 1)
	s.Set([]byte("k"), []byte("first"))
	v, _, _ := s.Get([]byte("k"))
	s.Set([]byte("k"), []byte("second"))
	if string(v) != "first" {
		t.Fatalf("retained slice = %q want first (write mutated a published entry)", v)
	}
}

// TestConcurrentReadersWriters hammers the same keyspace with many readers and
// writers at once. With -race this is the check that lock-free reads against
// in-place writes, deletes, and grow stay memory-safe and never read torn state.
func TestConcurrentReadersWriters(t *testing.T) {
	s := newStore(t, 16)
	const keys = 2000

	// Seed every key with a known-length value so a reader can assert structure.
	for i := 0; i < keys; i++ {
		s.Set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("seed-%06d", i)))
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writers: overwrite and occasionally delete+reinsert, driving grow and
	// tombstone churn under the readers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint32(seed*2654435761 + 1)
			for !stop.Load() {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				i := int(x) % keys
				k := []byte(fmt.Sprintf("k%d", i))
				if x&7 == 0 {
					s.Delete(k)
					s.Set(k, []byte(fmt.Sprintf("re-%06d", i)))
				} else {
					s.Set(k, []byte(fmt.Sprintf("new-%06d", i)))
				}
			}
		}(w)
	}

	// Readers: every hit must be one of the legal values for that key, never a
	// torn or out-of-range slice.
	var reads atomic.Int64
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint32(seed*40503 + 7)
			for !stop.Load() {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				i := int(x) % keys
				v, ok, _ := s.Get([]byte(fmt.Sprintf("k%d", i)))
				if ok {
					sv := string(v)
					if sv != fmt.Sprintf("seed-%06d", i) &&
						sv != fmt.Sprintf("new-%06d", i) &&
						sv != fmt.Sprintf("re-%06d", i) {
						t.Errorf("k%d torn value %q", i, sv)
						return
					}
				}
				reads.Add(1)
			}
		}(r)
	}

	for n := 0; n < 200_000; n++ {
		_, _, _ = s.Get([]byte(fmt.Sprintf("k%d", n%keys)))
	}
	stop.Store(true)
	wg.Wait()
	if reads.Load() == 0 {
		t.Fatal("no reads ran")
	}
}
