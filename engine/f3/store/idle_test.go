package store

import "testing"

// TestIdleSecondsClock drives the per-key access clock OBJECT IDLETIME reads
// back, with an explicit batch clock so the elapsed seconds are exact and the
// test never sleeps. A fresh write starts idle at zero, idle grows one-for-one
// with the seconds between the last touch and the query, and both a read
// (GetView) and a write (SetString, in-place IncrBy) restamp the clock the way
// Redis restamps robj.lru on every access.
func TestIdleSecondsClock(t *testing.T) {
	s := newTestStore()
	const sec = int64(1000) // ms per second
	base := int64(1_000_000) * sec

	if _, ok := s.IdleSeconds([]byte("k"), base); ok {
		t.Fatalf("IdleSeconds on absent key reported present")
	}

	if err := s.SetString([]byte("k"), []byte("hello"), base, 0, false); err != nil {
		t.Fatal(err)
	}
	if idle, ok := s.IdleSeconds([]byte("k"), base); !ok || idle != 0 {
		t.Fatalf("just-set idle = %d, %v; want 0, true", idle, ok)
	}
	if idle, _ := s.IdleSeconds([]byte("k"), base+5*sec); idle != 5 {
		t.Fatalf("idle 5s after set = %d; want 5", idle)
	}

	// A read stamps the clock: idle resets to zero at the read time.
	if _, ok := s.GetView([]byte("k"), base+5*sec); !ok {
		t.Fatal("GetView missed live key")
	}
	if idle, _ := s.IdleSeconds([]byte("k"), base+5*sec); idle != 0 {
		t.Fatalf("idle right after read = %d; want 0", idle)
	}
	if idle, _ := s.IdleSeconds([]byte("k"), base+8*sec); idle != 3 {
		t.Fatalf("idle 3s after read = %d; want 3", idle)
	}

	// An overwrite stamps too.
	if err := s.SetString([]byte("k"), []byte("world"), base+8*sec, 0, false); err != nil {
		t.Fatal(err)
	}
	if idle, _ := s.IdleSeconds([]byte("k"), base+8*sec); idle != 0 {
		t.Fatalf("idle after overwrite = %d; want 0", idle)
	}

	// An in-place INCR on an existing int cell stamps.
	if _, err := s.IncrBy([]byte("c"), 1, base); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy([]byte("c"), 1, base+2*sec); err != nil {
		t.Fatal(err)
	}
	if idle, _ := s.IdleSeconds([]byte("c"), base+2*sec); idle != 0 {
		t.Fatalf("idle after in-place INCR = %d; want 0", idle)
	}
	if idle, _ := s.IdleSeconds([]byte("c"), base+2*sec+7*sec); idle != 7 {
		t.Fatalf("idle 7s after INCR = %d; want 7", idle)
	}
}

// TestIdleSecondsWrap pins the two edges of the sixteen-bit, one-second clock:
// an idle window that straddles the 65536-second wrap still reads exactly, while
// an idle longer than the window folds back to a smaller number, the documented
// fidelity price of holding the clock in the record header's spare 16 bits.
func TestIdleSecondsWrap(t *testing.T) {
	s := newTestStore()
	const sec = int64(1000)

	// Set just below the wrap, read 100s later across it: exact.
	near := int64(65500) * sec
	if err := s.SetString([]byte("w"), []byte("v"), near, 0, false); err != nil {
		t.Fatal(err)
	}
	if idle, _ := s.IdleSeconds([]byte("w"), near+100*sec); idle != 100 {
		t.Fatalf("idle across wrap = %d; want 100", idle)
	}

	// Idle past a full 65536s window folds back (65540 -> 4), the known ceiling.
	if err := s.SetString([]byte("o"), []byte("v"), 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if idle, _ := s.IdleSeconds([]byte("o"), 65540*sec); idle != 4 {
		t.Fatalf("idle past the wrap window = %d; want the wrapped 4", idle)
	}
}
