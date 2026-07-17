package store

import (
	"fmt"
	"testing"
)

// collectKeys drains RangeKeys into a set, copying each key since the slice the
// walk hands back aliases the arena or the cold scratch.
func collectKeys(s *Store, now int64) map[string]bool {
	got := make(map[string]bool)
	s.RangeKeys(now, func(key []byte) bool {
		got[string(key)] = true
		return true
	})
	return got
}

// TestRangeKeysYieldsEveryResidentKey walks a store of plain resident keys and
// checks every one shows up exactly once, across enough keys to force segment
// splits and overflow chains so the walk covers home buckets and the overflow
// slab both.
func TestRangeKeysYieldsEveryResidentKey(t *testing.T) {
	s := testStore(t, 8)

	if got := collectKeys(s, 0); len(got) != 0 {
		t.Fatalf("fresh store yielded %d keys, want 0", len(got))
	}

	const n = 400
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key:%05d", i)
		if err := s.Set([]byte(k), []byte("v")); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		want[k] = true
	}

	got := collectKeys(s, 0)
	if len(got) != n {
		t.Fatalf("walk yielded %d keys, want %d", len(got), n)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("walk missed key %q", k)
		}
	}

	// A delete leaves a hole the walk skips: the deleted key stops showing and
	// the count drops by one.
	if !s.Delete([]byte("key:00000")) {
		t.Fatal("delete reported absent")
	}
	got = collectKeys(s, 0)
	if got["key:00000"] {
		t.Fatal("deleted key still walked")
	}
	if len(got) != n-1 {
		t.Fatalf("walk after delete yielded %d keys, want %d", len(got), n-1)
	}
}

// TestRangeKeysSkipsExpired checks the walk honors the lazy-expiry rule: a key
// whose deadline is at or before the walk clock is skipped, while a key with no
// deadline and one with a future deadline both show. Passing a zero clock skips
// the check, so the expired key reappears.
func TestRangeKeysSkipsExpired(t *testing.T) {
	s := testStore(t, 2)

	if err := s.SetString([]byte("plain"), []byte("v"), 1000, 0, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString([]byte("soon"), []byte("v"), 1000, 2000, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString([]byte("later"), []byte("v"), 1000, 9000, false); err != nil {
		t.Fatal(err)
	}

	// At clock 3000 the "soon" deadline has passed; "plain" and "later" stand.
	got := collectKeys(s, 3000)
	if got["soon"] {
		t.Fatal("expired key walked with a live clock")
	}
	if !got["plain"] || !got["later"] {
		t.Fatalf("walk dropped a live key: %v", got)
	}

	// A zero clock disables the expiry skip, so every key shows regardless of
	// its deadline: the clockless callers (no expiry semantics) see them all.
	got = collectKeys(s, 0)
	if !got["soon"] || !got["plain"] || !got["later"] {
		t.Fatalf("zero clock dropped a key: %v", got)
	}
}

// TestRangeKeysWalksColdEntries floods a capped store so the migrator demotes a
// slab of keys to the cold region, then checks the walk still yields every key,
// which it can only do by decoding the cold-tier slots through a frame pread.
func TestRangeKeysWalksColdEntries(t *testing.T) {
	const cap = 2 << 20
	s := migratorStore(t, cap)
	const n = 60000
	fillSmall(t, s, n)

	moved := s.MigrateCold()
	if moved == 0 {
		t.Fatal("fixture demoted no records: cold tier untested")
	}

	got := collectKeys(s, 0)
	if len(got) != n {
		t.Fatalf("walk yielded %d keys, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k:%07d", i)
		if !got[k] {
			t.Fatalf("walk missed key %q after demotion", k)
		}
	}
}

// TestRangeKeysStopsEarly checks fn returning false halts the walk: the
// callback stops after the first key, so exactly one is delivered.
func TestRangeKeysStopsEarly(t *testing.T) {
	s := testStore(t, 2)
	for i := 0; i < 50; i++ {
		if err := s.Set(fmt.Appendf(nil, "k:%03d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	seen := 0
	s.RangeKeys(0, func(key []byte) bool {
		seen++
		return false
	})
	if seen != 1 {
		t.Fatalf("early stop delivered %d keys, want 1", seen)
	}
}
