package f1raw

import (
	"encoding/binary"
	"sync"
	"testing"
)

// countKind is a spare kind byte for the count-record tests, disjoint from the kinds the
// other tests use so a probe never crosses namespaces.
const countKind byte = 0x77

// putCount writes an 8-byte little-endian counter n as key's value in the countKind
// namespace, the shape a collection header row carries in its first eight value bytes.
func putCount(t *testing.T, s *Store, key string, n int64) {
	t.Helper()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(n))
	if _, err := s.PutKind([]byte(key), b[:], countKind); err != nil {
		t.Fatalf("PutKind %q: %v", key, err)
	}
}

// TestCountAddInt64 covers the single-threaded read-modify-write contract: a present
// record adds and reports the running value, an absent record reports not-present, and a
// CountInt64 reads back exactly what the last add left.
func TestCountAddInt64(t *testing.T) {
	s := New(1<<10, 1<<20)

	if _, ok := s.CountAddInt64([]byte("missing"), countKind, -1); ok {
		t.Fatalf("CountAddInt64 on an absent record should report not-present")
	}
	if _, ok := s.CountInt64([]byte("missing"), countKind); ok {
		t.Fatalf("CountInt64 on an absent record should report not-present")
	}

	putCount(t, s, "c", 10)
	if n, ok := s.CountInt64([]byte("c"), countKind); !ok || n != 10 {
		t.Fatalf("CountInt64 after put: got %d,%v want 10,true", n, ok)
	}
	if n, ok := s.CountAddInt64([]byte("c"), countKind, -3); !ok || n != 7 {
		t.Fatalf("CountAddInt64 -3: got %d,%v want 7,true", n, ok)
	}
	if n, ok := s.CountAddInt64([]byte("c"), countKind, 5); !ok || n != 12 {
		t.Fatalf("CountAddInt64 +5: got %d,%v want 12,true", n, ok)
	}
	if n, ok := s.CountInt64([]byte("c"), countKind); !ok || n != 12 {
		t.Fatalf("CountInt64 final: got %d,%v want 12,true", n, ok)
	}
	if n, ok := s.CountAddInt64([]byte("c"), countKind, -12); !ok || n != 0 {
		t.Fatalf("CountAddInt64 to zero: got %d,%v want 0,true", n, ok)
	}
}

// TestCountAddInt64Concurrent pins the serialized-writer contract the stripe lock gives the
// server: N goroutines each holding a shared mutex decrement one shared counter, and the
// final value is exactly the starting value minus the total decremented, with no lost
// updates. This is the property SREM relies on when it charges one CountAddInt64 per removed
// member under the key's stripe lock.
func TestCountAddInt64Concurrent(t *testing.T) {
	s := New(1<<10, 1<<20)
	const start = 100000
	putCount(t, s, "c", start)

	const workers = 8
	const perWorker = start / workers // 12500 each, 100000 total
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				mu.Lock()
				s.CountAddInt64([]byte("c"), countKind, -1)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if n, ok := s.CountInt64([]byte("c"), countKind); !ok || n != 0 {
		t.Fatalf("after %d serialized decrements: got %d,%v want 0,true", start, n, ok)
	}
}

// TestCountReadUnderWriter runs a lock-free reader against a serialized writer to exercise
// the seqlock: while one goroutine decrements the counter one tick at a time (under its own
// lock, as the server would), a reader loops CountInt64 and must only ever observe values on
// the monotone descending path the writer actually published, never a torn number off it.
func TestCountReadUnderWriter(t *testing.T) {
	if raceEnabled {
		t.Skip("the seqlock count read/write is a benign race the detector cannot model; runs as a plain stress test")
	}
	s := New(1<<10, 1<<20)
	const start = 200000
	putCount(t, s, "c", start)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < start; i++ {
			s.CountAddInt64([]byte("c"), countKind, -1)
		}
		close(done)
	}()

	// Reader: every observed value must be in [0, start] and never increase, because the
	// only writer decrements. A torn read would land outside that window or move backward's
	// invariant, so this catches a seqlock miss.
	prev := int64(start)
	for {
		n, ok := s.CountInt64([]byte("c"), countKind)
		if !ok {
			t.Fatalf("record vanished mid-run")
		}
		if n < 0 || n > start {
			t.Fatalf("torn read: %d out of [0,%d]", n, start)
		}
		if n > prev {
			t.Fatalf("count moved up: %d after %d, but the only writer decrements", n, prev)
		}
		prev = n
		select {
		case <-done:
			if final, _ := s.CountInt64([]byte("c"), countKind); final != 0 {
				t.Fatalf("final count %d want 0", final)
			}
			wg.Wait()
			return
		default:
		}
	}
}
