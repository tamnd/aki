package keyspace

import (
	"fmt"
	"testing"
)

// drainScan runs Scan to completion from cursor 0 with the given count and
// returns every key seen, in the order the scan emitted them. It fails the test
// if the scan does not terminate within a generous call budget, which catches a
// cursor that fails to advance.
func drainScan(t *testing.T, db *DB, count int) []string {
	t.Helper()
	var got []string
	cursor := uint64(0)
	for calls := 0; ; calls++ {
		if calls > 4*numSlots {
			t.Fatalf("Scan did not terminate after %d calls (cursor stuck at %d)", calls, cursor)
		}
		next, entries, err := db.Scan(cursor, count)
		if err != nil {
			t.Fatalf("Scan(%d,%d): %v", cursor, count, err)
		}
		for _, e := range entries {
			got = append(got, string(e.Key))
		}
		if next == 0 {
			break
		}
		if next <= cursor {
			t.Fatalf("Scan cursor did not advance: %d -> %d", cursor, next)
		}
		cursor = next
	}
	return got
}

// TestScanBtreeFullCoverage checks the slot-window Scan returns every live key
// exactly once across a full drain, for a spread of count values including a
// count smaller than the keyspace, a count larger than it, and count 1. Coverage
// and no-duplication are the SCAN contract for keys present for the whole scan.
func TestScanBtreeFullCoverage(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	const n = 3000
	want := map[string]bool{}
	for i := range n {
		k := fmt.Sprintf("key:%05d:%d", i, i*7)
		want[k] = true
		if err := db.Set([]byte(k), []byte("v"), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	for _, count := range []int{1, 7, 10, 256, n + 100} {
		got := drainScan(t, db, count)
		seen := map[string]bool{}
		for _, k := range got {
			if seen[k] {
				t.Fatalf("count=%d: key %q returned twice", count, k)
			}
			seen[k] = true
			if !want[k] {
				t.Fatalf("count=%d: key %q not in the keyspace", count, k)
			}
		}
		if len(seen) != len(want) {
			t.Fatalf("count=%d: scan covered %d keys, want %d", count, len(seen), len(want))
		}
	}
}

// TestScanBtreeIsBounded is the OOM witness: a single Scan call over a keyspace
// far larger than the page it returns must allocate on the order of the page, not
// the keyspace. The old path collected every key into a slice (two clones each)
// and sorted it, so one call allocated O(n); the slot-window path seeks straight
// to its first slot and walks only enough slots to fill count. The allocation
// count is asserted well under what a whole-keyspace materialize of n keys would
// reach, and it does not grow when the keyspace doubles.
func TestScanBtreeIsBounded(t *testing.T) {
	if raceEnabled {
		t.Skip("testing.AllocsPerRun is unreliable under -race; the bound is checked on a normal build")
	}
	build := func(n int) *DB {
		ks, _, _ := newKS(t)
		db := mustDB(t, ks, 0)
		for i := range n {
			k := fmt.Sprintf("k:%07d", i)
			if err := db.Set([]byte(k), []byte("v"), TypeString, EncRaw, -1); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		return db
	}

	const count = 10
	measure := func(db *DB) float64 {
		return testing.AllocsPerRun(20, func() {
			if _, _, err := db.Scan(0, count); err != nil {
				t.Fatalf("Scan: %v", err)
			}
		})
	}

	small := measure(build(20000))
	large := measure(build(40000))

	// A whole-keyspace materialize would allocate tens of thousands of objects for
	// these sizes; a count=10 page touches a handful of dense slots.
	if small > 800 {
		t.Fatalf("Scan(0,%d) over 20000 keys allocated %.0f objects per run; "+
			"a bounded page should track count, not the keyspace size", count, small)
	}
	// Doubling the keyspace must not grow the per-call allocation: the page is the
	// same size and the denser slots are reached with no more work.
	if large > small*2+200 {
		t.Fatalf("Scan(0,%d) allocated %.0f over 40000 keys vs %.0f over 20000; "+
			"per-call cost must not scale with the keyspace", count, large, small)
	}
}

// TestScanBtreeStableUnderDelete checks that keys present for the whole scan are
// each returned exactly once even when other keys are deleted between calls, the
// guarantee the slot cursor gives because a key's slot is a pure function of the
// key and never moves.
func TestScanBtreeStableUnderDelete(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	const n = 2000
	stable := map[string]bool{}
	var volatileKeys []string
	for i := range n {
		k := fmt.Sprintf("k:%05d", i)
		if err := db.Set([]byte(k), []byte("v"), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if i%2 == 0 {
			stable[k] = true
		} else {
			volatileKeys = append(volatileKeys, k)
		}
	}

	seen := map[string]bool{}
	cursor := uint64(0)
	deleted := 0
	for {
		next, entries, err := db.Scan(cursor, 16)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, e := range entries {
			k := string(e.Key)
			if stable[k] {
				if seen[k] {
					t.Fatalf("stable key %q returned twice across a delete", k)
				}
				seen[k] = true
			}
		}
		// Delete one volatile key per call to mutate the keyspace mid-scan.
		if deleted < len(volatileKeys) {
			if _, err := db.Delete([]byte(volatileKeys[deleted])); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			deleted++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != len(stable) {
		t.Fatalf("scan saw %d stable keys, want all %d", len(seen), len(stable))
	}
}

// TestScanBtreeSkipsExpired checks an expired key is not emitted, matching
// forEachLive's lazy-expiry skip, and that the type field rides through.
func TestScanBtreeSkipsExpired(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	if err := db.Set([]byte("live"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("Set live: %v", err)
	}
	// A TTL one millisecond in the past is already expired.
	if err := db.Set([]byte("dead"), []byte("v"), TypeString, EncRaw, 1); err != nil {
		t.Fatalf("Set dead: %v", err)
	}

	got := drainScan(t, db, 10)
	for _, k := range got {
		if k == "dead" {
			t.Fatalf("scan returned an expired key")
		}
	}
	if len(got) != 1 || got[0] != "live" {
		t.Fatalf("scan returned %v, want only [live]", got)
	}
}

// TestScanBtreeEmpty checks an empty keyspace finishes in one bounded sweep with
// cursor 0 and no keys, rather than walking forever.
func TestScanBtreeEmpty(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	got := drainScan(t, db, 10)
	if len(got) != 0 {
		t.Fatalf("empty keyspace scan returned %v", got)
	}
	// The empty-shard short-circuit means the whole numSlots range clears in a few
	// budget-sized sweeps; drainScan already asserts termination.
}

// TestScanBtreeTypeField checks ScanEntry.Type reports the stored value type so a
// TYPE filter needs no second lookup.
func TestScanBtreeTypeField(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	if err := db.Set([]byte("s"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Drain the scan and check the type carried on the entry for "s", wherever its
	// slot places it.
	var found bool
	cursor := uint64(0)
	for {
		next, entries, err := db.Scan(cursor, 16)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, e := range entries {
			if string(e.Key) == "s" {
				found = true
				if e.Type != TypeString {
					t.Fatalf("ScanEntry.Type = %d, want TypeString %d", e.Type, TypeString)
				}
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if !found {
		t.Fatalf("scan never returned key s")
	}
}
